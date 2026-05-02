using Microsoft.Extensions.DependencyInjection;

namespace Mesh.Tests;

public sealed record OrderPlaced(string OrderId, decimal Total);

[EventName("keep.secret-rotated")]
public sealed record SecretRotated(string Ref, int Version);

public sealed class OrderHandler(Log log) : IEventHandler<OrderPlaced>
{
    public Task HandleAsync(OrderPlaced e, EventContext c, CancellationToken ct) { log.Items.Add($"di:{e.OrderId}:{c.TenantId}"); return Task.CompletedTask; }
}

public sealed class Log { public List<string> Items { get; } = []; }

public class MeshTests
{
    private static ServiceProvider Build(Action<IServiceCollection>? more = null, Action<MeshOptions>? o = null)
    {
        var s = new ServiceCollection();
        s.AddLogging();
        s.AddSingleton<Log>();
        s.AddMesh(o);
        more?.Invoke(s);
        return s.BuildServiceProvider();
    }

    [Fact]
    public async Task Subscriber_receives_typed_event_with_context()
    {
        using var sp = Build();
        var bus = sp.GetRequiredService<IEventBus>();
        OrderPlaced? got = null; EventContext? ctx = null;
        bus.Subscribe<OrderPlaced>((e, c, _) => { got = e; ctx = c; return Task.CompletedTask; });

        await bus.PublishAsync(new OrderPlaced("o1", 9.5m), new PublishOptions { TenantId = "t1", CorrelationId = "c1", Headers = new Dictionary<string, string> { ["k"] = "v" } });

        Assert.Equal(new OrderPlaced("o1", 9.5m), got);
        Assert.Equal("t1", ctx!.TenantId);
        Assert.Equal("c1", ctx.CorrelationId);
        Assert.Equal("v", ctx.Headers["k"]);
        Assert.NotEqual(Guid.Empty, ctx.EventId);
    }

    [Fact]
    public async Task Only_matching_type_is_delivered_and_multiple_handlers_all_run()
    {
        using var sp = Build();
        var bus = sp.GetRequiredService<IEventBus>();
        var calls = new List<string>();
        bus.Subscribe<OrderPlaced>((_, _, _) => { calls.Add("a"); return Task.CompletedTask; });
        bus.Subscribe<OrderPlaced>((_, _, _) => { calls.Add("b"); return Task.CompletedTask; });
        bus.Subscribe<SecretRotated>((_, _, _) => { calls.Add("other"); return Task.CompletedTask; });

        await bus.PublishAsync(new OrderPlaced("o", 1));
        Assert.Equal(["a", "b"], calls);
    }

    [Fact]
    public async Task Unsubscribe_stops_delivery()
    {
        using var sp = Build();
        var bus = sp.GetRequiredService<IEventBus>();
        var n = 0;
        var sub = bus.Subscribe<OrderPlaced>((_, _, _) => { n++; return Task.CompletedTask; });
        await bus.PublishAsync(new OrderPlaced("1", 1));
        sub.Dispose();
        await bus.PublishAsync(new OrderPlaced("2", 1));
        Assert.Equal(1, n);
    }

    [Fact]
    public async Task Failing_handler_does_not_affect_publisher_or_other_handlers()
    {
        using var sp = Build();
        var bus = sp.GetRequiredService<IEventBus>();
        var ran = false;
        bus.Subscribe<OrderPlaced>((_, _, _) => throw new InvalidOperationException("bad"));
        bus.Subscribe<OrderPlaced>((_, _, _) => { ran = true; return Task.CompletedTask; });
        await bus.PublishAsync(new OrderPlaced("1", 1));
        Assert.True(ran);
    }

    [Fact]
    public async Task Throw_mode_aggregates_failures_after_running_all_handlers()
    {
        using var sp = Build(o: o => o.FailureMode = HandlerFailureMode.Throw);
        var bus = sp.GetRequiredService<IEventBus>();
        var ran = false;
        bus.Subscribe<OrderPlaced>((_, _, _) => throw new InvalidOperationException("bad"));
        bus.Subscribe<OrderPlaced>((_, _, _) => { ran = true; return Task.CompletedTask; });
        var ex = await Assert.ThrowsAsync<AggregateException>(() => bus.PublishAsync(new OrderPlaced("1", 1)));
        Assert.True(ran);
        Assert.Single(ex.InnerExceptions);
    }

    [Fact]
    public async Task EventName_attribute_sets_logical_name_and_raw_subscription_sees_it()
    {
        using var sp = Build();
        var bus = sp.GetRequiredService<IEventBus>();
        var raw = new List<string>();
        bus.SubscribeRaw("keep.secret-rotated", (e, _) => { raw.Add(e.EventName); return Task.CompletedTask; });
        bus.SubscribeRaw(null, (e, _) => { raw.Add("all:" + e.EventName); return Task.CompletedTask; });

        await bus.PublishAsync(new SecretRotated("db", 2));
        await bus.PublishAsync(new OrderPlaced("1", 1));

        Assert.Equal(["keep.secret-rotated", "all:keep.secret-rotated", $"all:{typeof(OrderPlaced).FullName}"], raw);
    }

    [Fact]
    public async Task Di_handlers_run_in_a_scope_with_context()
    {
        using var sp = Build(s => s.AddMeshHandler<OrderPlaced, OrderHandler>());
        await sp.GetRequiredService<IEventBus>().PublishAsync(new OrderPlaced("o9", 1), new PublishOptions { TenantId = "tenantX" });
        Assert.Equal(["di:o9:tenantX"], sp.GetRequiredService<Log>().Items);
    }

    [Fact]
    public async Task Cancellation_propagates_to_handlers()
    {
        using var sp = Build();
        var bus = sp.GetRequiredService<IEventBus>();
        CancellationToken seen = default;
        bus.Subscribe<OrderPlaced>((_, _, ct) => { seen = ct; return Task.CompletedTask; });
        using var cts = new CancellationTokenSource();
        await bus.PublishAsync(new OrderPlaced("1", 1), cancellationToken: cts.Token);
        Assert.Equal(cts.Token, seen);
    }

    // --- Pluggable transport: a fake "broker" shared by two bus instances (simulating two app instances) ---
    private sealed class Broker { public List<BrokerTransport> Nodes { get; } = []; }

    private sealed class BrokerTransport(Broker broker) : IEventTransport
    {
        private IEventReceiver? _r;
        public void Attach(IEventReceiver receiver) { _r = receiver; broker.Nodes.Add(this); }
        public async Task PublishAsync(EventEnvelope e, CancellationToken ct)
        {
            foreach (var n in broker.Nodes) await n._r!.ReceiveAsync(e, ct);
        }
    }

    [Fact]
    public async Task Custom_transport_fans_out_to_other_instances_without_changing_publishers_or_subscribers()
    {
        var broker = new Broker();
        ServiceProvider Node() => Build(s => s.AddMeshTransport<BrokerTransport>().AddSingleton(broker));
        using var a = Node();
        using var b = Node();
        var got = new List<string>();
        a.GetRequiredService<IEventBus>().Subscribe<OrderPlaced>((e, _, _) => { got.Add("a:" + e.OrderId); return Task.CompletedTask; });
        b.GetRequiredService<IEventBus>().Subscribe<OrderPlaced>((e, _, _) => { got.Add("b:" + e.OrderId); return Task.CompletedTask; });

        await a.GetRequiredService<IEventBus>().PublishAsync(new OrderPlaced("x", 1));
        Assert.Equal(["a:x", "b:x"], got);
    }
}
