using Microsoft.Extensions.DependencyInjection;

namespace Mesh.Tests;

public sealed record LateNamed(string Id);

public sealed record GenericNamed(string Id);

public sealed class LateNamedHandler(Log log) : IEventHandler<LateNamed>
{
    public Task HandleAsync(LateNamed e, EventContext c, CancellationToken ct) { log.Items.Add($"{c.EventName}:{e.Id}"); return Task.CompletedTask; }
}

/// <summary>An adapter a product wraps its own handlers in: a generic handler type.</summary>
public sealed class Adapter<TEvent>(Log log) : IEventHandler<TEvent> where TEvent : notnull
{
    public Task HandleAsync(TEvent e, EventContext c, CancellationToken ct) { log.Items.Add($"adapter:{c.EventName}"); return Task.CompletedTask; }
}

public class HandlerKeyTests
{
    [Fact]
    public async Task A_handler_registered_before_its_event_is_named_gets_the_name()
    {
        var s = new ServiceCollection();
        s.AddLogging();
        s.AddSingleton<Log>();
        s.AddMesh();
        s.AddMeshHandler<LateNamed, LateNamedHandler>();      // a consuming module configured first
        s.AddMeshEventName<LateNamed>("sales.late_named");     // then the publishing one names the event
        using var sp = s.BuildServiceProvider();

        await sp.GetRequiredService<IEventBus>().PublishAsync(new LateNamed("x"));

        Assert.Equal(["sales.late_named:x"], sp.GetRequiredService<Log>().Items);
    }

    [Fact]
    public void Generic_handler_keys_carry_no_assembly_versions()
    {
        var key = HandlerKeys.Of(typeof(Adapter<GenericNamed>));
        Assert.Equal("Mesh.Tests.Adapter`1[Mesh.Tests.GenericNamed]", key);
        Assert.DoesNotContain("Version=", key);
        Assert.Equal(typeof(LateNamedHandler).FullName, HandlerKeys.Of(typeof(LateNamedHandler)));   // unchanged for plain types
    }

    [Fact]
    public async Task An_explicit_key_names_the_handler_for_durable_delivery()
    {
        var s = new ServiceCollection();
        s.AddLogging();
        s.AddSingleton<Log>();
        s.AddMesh();
        s.AddMeshEventName<GenericNamed>("sales.generic_named");
        s.AddMeshHandler<GenericNamed, Adapter<GenericNamed>>("sales.generic_named/reporting");
        using var sp = s.BuildServiceProvider();

        await sp.GetRequiredService<IEventBus>().PublishAsync(new GenericNamed("y"));

        Assert.Equal(["adapter:sales.generic_named"], sp.GetRequiredService<Log>().Items);
        Assert.Throws<ArgumentException>(() => new ServiceCollection().AddMeshHandler<GenericNamed, Adapter<GenericNamed>>(" "));
    }
}
