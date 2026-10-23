using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Npgsql;
using Relay;

namespace Mesh;

/// <summary>Options of the durable transport on Relay.</summary>
public sealed class MeshRelayOptions
{
    /// <summary>The Relay table transactional publishes write to (<c>table</c> or <c>schema.table</c>), on the caller's database.</summary>
    public string Table { get; set; } = PostgresRelayStore.DefaultTable;
}

/// <summary>
/// Publishes events inside the caller's own transaction (the transactional outbox): the event's handlers are queued in Relay with the
/// business change and delivered once it commits, each on its own and retried by Relay until it succeeds.
/// </summary>
public interface IMeshOutbox
{
    /// <summary>Queues the event for every DI handler of it, on <paramref name="connection"/> and <paramref name="transaction"/>. Returns the event id.</summary>
    Task<Guid> PublishAsync<TEvent>(NpgsqlConnection connection, NpgsqlTransaction? transaction, TEvent @event, PublishOptions? options = null,
        CancellationToken cancellationToken = default) where TEvent : notnull;
}

/// <summary>
/// Durable transport: one Relay message per DI handler (destination <c>mesh:{handler}</c>), so a failing handler is retried, and
/// dead-lettered, without running the others again. Delegate subscriptions are this process's own and run at once.
/// </summary>
internal sealed class RelayTransport(IServiceProvider services) : IEventTransport
{
    private EventBus? _bus;

    public void Attach(IEventReceiver receiver) => _bus = (EventBus)receiver;

    public async Task PublishAsync(EventEnvelope envelope, CancellationToken cancellationToken)
    {
        var bus = _bus ?? throw new InvalidOperationException("The transport is not attached to a bus.");
        var outbox = services.GetRequiredService<IRelayOutbox>();
        foreach (var handler in bus.HandlersOf(envelope.EventName))
        {
            await outbox.EnqueueAsync(MeshRelayMessages.Envelope(envelope, handler.Key), cancellationToken).ConfigureAwait(false);
        }

        await bus.ReceiveSubscriptionsAsync(envelope, cancellationToken).ConfigureAwait(false);
    }
}

internal sealed class MeshOutbox(EventBus bus, IOptions<MeshRelayOptions> options, TimeProvider time) : IMeshOutbox
{
    public async Task<Guid> PublishAsync<TEvent>(NpgsqlConnection connection, NpgsqlTransaction? transaction, TEvent @event, PublishOptions? publish = null,
        CancellationToken cancellationToken = default) where TEvent : notnull
    {
        ArgumentNullException.ThrowIfNull(connection);
        var envelope = bus.Envelope(@event, publish);
        var now = time.GetUtcNow();
        foreach (var handler in bus.HandlersOf(envelope.EventName))
        {
            var relay = MeshRelayMessages.Envelope(envelope, handler.Key);
            var message = new RelayMessage(relay.MessageId!.Value, relay.Destination, null, relay.Payload, new Dictionary<string, string>(), now, now, TenantId: relay.TenantId);
            await PostgresRelayStore.EnqueueInTransactionAsync(connection, transaction, message, options.Value.Table, cancellationToken).ConfigureAwait(false);
        }

        return envelope.Id;
    }
}

/// <summary>Relay's side: delivers <c>mesh:{handler}</c> messages to that handler; a failure is Relay's to retry.</summary>
internal sealed class MeshRelayDestination(EventBus bus) : IRelayDestination
{
    public string Name => MeshRelayMessages.Prefix.TrimEnd(':');

    public bool Handles(string destination) => destination.StartsWith(MeshRelayMessages.Prefix, StringComparison.Ordinal);

    public Task DeliverAsync(RelayMessage message, CancellationToken cancellationToken)
    {
        var envelope = JsonSerializer.Deserialize<EventEnvelope>(message.Payload, MeshRelayMessages.Json)
            ?? throw new RelayPermanentFailureException("The message holds no event envelope.");
        return bus.DeliverToHandlerAsync(message.Destination[MeshRelayMessages.Prefix.Length..], envelope, cancellationToken);
    }
}

internal static class MeshRelayMessages
{
    public const string Prefix = "mesh:";

    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    /// <summary>One handler's copy of an event; its id is derived from the event and the handler, so a repeated publish is a no-op.</summary>
    public static RelayEnvelope Envelope(EventEnvelope envelope, string handlerKey) => new()
    {
        Destination = Prefix + handlerKey,
        Payload = JsonSerializer.SerializeToUtf8Bytes(envelope, Json),
        TenantId = envelope.TenantId,
        MessageId = new Guid(SHA256.HashData(Encoding.UTF8.GetBytes($"{envelope.Id:N}/{handlerKey}")).AsSpan(0, 16)),
    };
}

public static class MeshRelayServiceCollectionExtensions
{
    /// <summary>
    /// Delivers the DI handlers durably through Relay (register Relay's stores, <c>AddRelay</c> and its dispatcher too), and adds
    /// <see cref="IMeshOutbox"/> for publishing inside a transaction. Handlers must be idempotent: delivery is at-least-once (de-duplicate by
    /// <see cref="EventContext.EventId"/>).
    /// </summary>
    public static IServiceCollection AddMeshRelay(this IServiceCollection services, Action<MeshRelayOptions>? configure = null)
    {
        var options = services.AddOptions<MeshRelayOptions>();
        if (configure is not null) options.Configure(configure);
        services.AddMeshTransport<RelayTransport>();
        services.AddMesh();
        services.TryAddSingleton<IMeshOutbox, MeshOutbox>();
        services.AddSingleton<IRelayDestination, MeshRelayDestination>();
        return services;
    }
}
