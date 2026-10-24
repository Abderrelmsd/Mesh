using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Mesh;

/// <summary>Options of the Postgres broadcast transport.</summary>
public sealed class MeshBroadcastOptions
{
    /// <summary>The <c>LISTEN</c>/<c>NOTIFY</c> channel every instance shares (a plain identifier).</summary>
    public string Channel { get; set; } = "mesh";

    /// <summary>Wait before reconnecting after the listening connection fails.</summary>
    public TimeSpan ReconnectDelay { get; set; } = TimeSpan.FromSeconds(2);
}

/// <summary>
/// Delivered to this instance's subscribers whenever the broadcast listener connects or reconnects. Notifications sent while it was
/// not listening are lost, so anything kept in step by broadcast events (a cache, say) should treat this as "everything may have changed".
/// </summary>
[EventName("mesh.broadcast-resync")]
public sealed record BroadcastResync(DateTimeOffset At);

/// <summary>
/// Broadcast transport on Postgres <c>LISTEN</c>/<c>NOTIFY</c>: every instance receives every event, the publisher included, so it suits
/// notifications every process must see (cache invalidation, configuration changes). Delivery is at-most-once and in commit order per
/// publisher; events published inside a transaction are only delivered if it commits (use <see cref="NotifyInTransactionAsync"/>).
/// Payloads are limited by Postgres to just under 8000 bytes, so send small notifications, not data.
/// </summary>
public sealed partial class PostgresBroadcastTransport : IEventTransport
{
    /// <summary>Postgres rejects NOTIFY payloads of 8000 bytes or more.</summary>
    public const int MaxPayloadBytes = 7999;

    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private readonly NpgsqlDataSource _dataSource;
    private readonly MeshBroadcastOptions _options;
    private readonly TaskCompletionSource _listening = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private IEventReceiver? _receiver;

    public PostgresBroadcastTransport(NpgsqlDataSource dataSource, IOptions<MeshBroadcastOptions> options)
    {
        _dataSource = dataSource;
        _options = options.Value;
        if (!ChannelName().IsMatch(_options.Channel))
            throw new ArgumentException($"'{_options.Channel}' is not a valid channel name (letters, digits and underscores).");
    }

    /// <summary>Completes once the listener has issued its first <c>LISTEN</c> (useful in tests and readiness checks).</summary>
    public Task Listening => _listening.Task;

    public void Attach(IEventReceiver receiver) => _receiver = receiver;

    public async Task PublishAsync(EventEnvelope envelope, CancellationToken cancellationToken)
    {
        await using var cmd = _dataSource.CreateCommand("SELECT pg_notify(@channel, @payload)");
        Bind(cmd, envelope);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Notifies on the caller's transaction: Postgres delivers it only when the transaction commits.</summary>
    public async Task NotifyInTransactionAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, EventEnvelope envelope,
        CancellationToken cancellationToken = default)
    {
        await using var cmd = new NpgsqlCommand("SELECT pg_notify(@channel, @payload)", connection, transaction);
        Bind(cmd, envelope);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private void Bind(NpgsqlCommand cmd, EventEnvelope envelope)
    {
        var payload = JsonSerializer.Serialize(envelope, Json);
        if (Encoding.UTF8.GetByteCount(payload) > MaxPayloadBytes)
            throw new InvalidOperationException(
                $"Event '{envelope.EventName}' is too large to broadcast ({MaxPayloadBytes} bytes at most). Broadcast a notification and let receivers load the data.");
        cmd.Parameters.AddWithValue("channel", _options.Channel);
        cmd.Parameters.AddWithValue("payload", payload);
    }

    internal string Channel => _options.Channel;
    internal TimeSpan ReconnectDelay => _options.ReconnectDelay;
    internal NpgsqlDataSource DataSource => _dataSource;

    internal void MarkListening() => _listening.TrySetResult();

    internal Task DeliverAsync(string payload, CancellationToken ct)
    {
        var envelope = JsonSerializer.Deserialize<EventEnvelope>(payload, Json)
            ?? throw new InvalidOperationException("Empty broadcast payload.");
        return _receiver?.ReceiveAsync(envelope, ct) ?? Task.CompletedTask;
    }

    internal Task DeliverLocalAsync(EventEnvelope envelope, CancellationToken ct) => _receiver?.ReceiveAsync(envelope, ct) ?? Task.CompletedTask;

    [GeneratedRegex("^[A-Za-z_][A-Za-z0-9_]{0,62}$")]
    private static partial Regex ChannelName();
}

/// <summary>Holds the <c>LISTEN</c> connection and hands each notification to the bus; reconnects (and announces a resync) after failures.</summary>
internal sealed class PostgresBroadcastListener(PostgresBroadcastTransport transport, IEventBus bus, TimeProvider time,
    ILogger<PostgresBroadcastListener> logger) : BackgroundService
{
    // The bus attaches itself to the transport when it is created. A process whose only subscribers are DI handlers may never ask for it,
    // so the listener does: without a bus, received events would have nowhere to go.
    private readonly IEventBus _bus = bus;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        GC.KeepAlive(_bus);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ListenAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Broadcast listener on channel {Channel} failed; reconnecting", transport.Channel);
                try { await Task.Delay(transport.ReconnectDelay, time, stoppingToken).ConfigureAwait(false); }
                catch (OperationCanceledException) { return; }
            }
        }
    }

    private async Task ListenAsync(CancellationToken ct)
    {
        await using var connection = await transport.DataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
        var received = new List<string>();
        connection.Notification += (_, e) => { if (e.Channel == transport.Channel) received.Add(e.Payload); };

        await using (var listen = new NpgsqlCommand($"LISTEN \"{transport.Channel}\"", connection))
            await listen.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        transport.MarkListening();

        // Anything published while this instance was not listening is lost: tell local subscribers to resynchronise.
        var resync = new EventEnvelope(Guid.NewGuid(), EventBus.NameOf(typeof(BroadcastResync)),
            JsonSerializer.SerializeToUtf8Bytes(new BroadcastResync(time.GetUtcNow()), new JsonSerializerOptions(JsonSerializerDefaults.Web)),
            time.GetUtcNow());
        await transport.DeliverLocalAsync(resync, ct).ConfigureAwait(false);

        while (true)
        {
            await connection.WaitAsync(ct).ConfigureAwait(false);
            foreach (var payload in received.ToArray())
            {
                try
                {
                    await transport.DeliverAsync(payload, ct).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    logger.LogError(ex, "Could not deliver a broadcast event from channel {Channel}", transport.Channel);
                }
            }
            received.Clear();
        }
    }
}

public static class MeshBroadcastExtensions
{
    /// <summary>
    /// Makes the bus broadcast over Postgres <c>LISTEN</c>/<c>NOTIFY</c> (<see cref="PostgresBroadcastTransport"/>) and starts the listener.
    /// <paramref name="dataSource"/> picks the database every instance shares.
    /// </summary>
    public static IServiceCollection AddMeshPostgresBroadcast(this IServiceCollection services, Func<IServiceProvider, NpgsqlDataSource> dataSource,
        Action<MeshBroadcastOptions>? configure = null)
    {
        var options = services.AddOptions<MeshBroadcastOptions>();
        if (configure is not null) options.Configure(configure);
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton(sp => new PostgresBroadcastTransport(dataSource(sp), sp.GetRequiredService<IOptions<MeshBroadcastOptions>>()));
        services.RemoveAll<IEventTransport>();
        services.AddSingleton<IEventTransport>(sp => sp.GetRequiredService<PostgresBroadcastTransport>());
        services.AddHostedService<PostgresBroadcastListener>();
        return services;
    }
}
