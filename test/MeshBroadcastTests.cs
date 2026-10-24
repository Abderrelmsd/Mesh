using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Mesh.Tests;

[EventName("test.settings-changed")]
public sealed record SettingsChanged(string Key);

public class MeshBroadcastTests(PgFixture pg) : IClassFixture<PgFixture>
{
    private sealed class Instance : IAsyncDisposable
    {
        public required ServiceProvider Services { get; init; }
        public required IHostedService Listener { get; init; }
        public IEventBus Bus => Services.GetRequiredService<IEventBus>();
        public PostgresBroadcastTransport Transport => Services.GetRequiredService<PostgresBroadcastTransport>();

        public async ValueTask DisposeAsync()
        {
            await Listener.StopAsync(CancellationToken.None);
            await Services.DisposeAsync();
        }
    }

    private async Task<Instance> StartAsync(string channel)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMesh(o => o.FailureMode = HandlerFailureMode.Throw);
        services.AddMeshPostgresBroadcast(_ => pg.DataSource!, o => o.Channel = channel);
        var sp = services.BuildServiceProvider();
        var listener = sp.GetServices<IHostedService>().Single();
        await listener.StartAsync(CancellationToken.None);
        await sp.GetRequiredService<PostgresBroadcastTransport>().Listening.WaitAsync(TimeSpan.FromSeconds(10));
        return new Instance { Services = sp, Listener = listener };
    }

    private static async Task Eventually(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) Assert.Fail("Condition not met within 10 seconds.");
            await Task.Delay(25);
        }
    }

    [Fact]
    public async Task Every_instance_receives_every_event_including_the_publisher()
    {
        Assert.SkipUnless(pg.Available, pg.Reason);
        var channel = "c" + Guid.NewGuid().ToString("N")[..12];
        await using var a = await StartAsync(channel);
        await using var b = await StartAsync(channel);
        var seenByA = new ConcurrentQueue<string>();
        var seenByB = new ConcurrentQueue<string>();
        a.Bus.Subscribe<SettingsChanged>((e, _, _) => { seenByA.Enqueue(e.Key); return Task.CompletedTask; });
        b.Bus.Subscribe<SettingsChanged>((e, _, _) => { seenByB.Enqueue(e.Key); return Task.CompletedTask; });

        await a.Bus.PublishAsync(new SettingsChanged("one"), new PublishOptions { TenantId = "t1" });
        await b.Bus.PublishAsync(new SettingsChanged("two"));

        await Eventually(() => seenByA.Count == 2 && seenByB.Count == 2);
        Assert.Equal(["one", "two"], seenByA);
        Assert.Equal(["one", "two"], seenByB);
    }

    public sealed class Recorder : IEventHandler<SettingsChanged>
    {
        public static readonly ConcurrentQueue<string> Seen = new();
        public Task HandleAsync(SettingsChanged @event, EventContext context, CancellationToken cancellationToken)
        {
            Seen.Enqueue(@event.Key);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task Di_handlers_receive_broadcasts_even_when_nothing_resolved_the_bus()
    {
        Assert.SkipUnless(pg.Available, pg.Reason);
        var channel = "c" + Guid.NewGuid().ToString("N")[..12];
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMesh();
        services.AddMeshPostgresBroadcast(_ => pg.DataSource!, o => o.Channel = channel);
        services.AddMeshHandler<SettingsChanged, Recorder>();
        await using var sp = services.BuildServiceProvider();
        var listener = sp.GetServices<IHostedService>().Single();
        await listener.StartAsync(CancellationToken.None);
        await sp.GetRequiredService<PostgresBroadcastTransport>().Listening.WaitAsync(TimeSpan.FromSeconds(10));

        var key = Guid.NewGuid().ToString("N");
        await using var publisher = await StartAsync(channel);
        await publisher.Bus.PublishAsync(new SettingsChanged(key));

        await Eventually(() => Recorder.Seen.Contains(key));
        await listener.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Subscribers_hear_a_resync_when_the_listener_connects()
    {
        Assert.SkipUnless(pg.Available, pg.Reason);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMesh();
        services.AddMeshPostgresBroadcast(_ => pg.DataSource!, o => o.Channel = "c" + Guid.NewGuid().ToString("N")[..12]);
        await using var sp = services.BuildServiceProvider();
        var resyncs = 0;
        sp.GetRequiredService<IEventBus>().Subscribe<BroadcastResync>((_, _, _) => { Interlocked.Increment(ref resyncs); return Task.CompletedTask; });

        var listener = sp.GetServices<IHostedService>().Single();
        await listener.StartAsync(CancellationToken.None);
        await Eventually(() => Volatile.Read(ref resyncs) == 1);
        await listener.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Notifications_in_a_transaction_are_delivered_only_on_commit()
    {
        Assert.SkipUnless(pg.Available, pg.Reason);
        var channel = "c" + Guid.NewGuid().ToString("N")[..12];
        await using var a = await StartAsync(channel);
        var seen = new ConcurrentQueue<string>();
        a.Bus.Subscribe<SettingsChanged>((e, _, _) => { seen.Enqueue(e.Key); return Task.CompletedTask; });

        await using var connection = await pg.DataSource!.OpenConnectionAsync();
        await using (var rolledBack = await connection.BeginTransactionAsync())
        {
            await a.Transport.NotifyInTransactionAsync(connection, rolledBack, Envelope("rolled-back"));
            await rolledBack.RollbackAsync();
        }
        await using (var committed = await connection.BeginTransactionAsync())
        {
            await a.Transport.NotifyInTransactionAsync(connection, committed, Envelope("committed"));
            await committed.CommitAsync();
        }

        await Eventually(() => !seen.IsEmpty);
        await Task.Delay(200);
        Assert.Equal(["committed"], seen);
    }

    [Fact]
    public async Task Oversized_events_are_refused_before_reaching_the_database()
    {
        await using var ds = NpgsqlDataSource.Create("Host=unreachable.invalid;Username=x;Password=x");
        var transport = new PostgresBroadcastTransport(ds, Options.Create(new MeshBroadcastOptions()));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => transport.PublishAsync(Envelope(new string('x', 8000)), CancellationToken.None));
        Assert.Contains("too large to broadcast", ex.Message);
    }

    [Fact]
    public void Channel_names_must_be_plain_identifiers()
    {
        using var ds = NpgsqlDataSource.Create("Host=unreachable.invalid;Username=x;Password=x");
        Assert.Throws<ArgumentException>(() => new PostgresBroadcastTransport(ds, Options.Create(new MeshBroadcastOptions { Channel = "a\"; DROP" })));
    }

    private static EventEnvelope Envelope(string key)
        => new(Guid.NewGuid(), "test.settings-changed", System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new SettingsChanged(key),
            new System.Text.Json.JsonSerializerOptions(System.Text.Json.JsonSerializerDefaults.Web)), DateTimeOffset.UtcNow);
}
