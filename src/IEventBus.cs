namespace Mesh;

/// <summary>Publish/subscribe for domain events without coupling publishers to subscribers.</summary>
public interface IEventBus
{
    /// <summary>
    /// Publishes an event. Delivery is at-least-once on durable transports; use <see cref="EventContext.EventId"/> to de-duplicate.
    /// With the in-process transport, handlers have run by the time the returned task completes.
    /// </summary>
    Task PublishAsync<TEvent>(TEvent @event, PublishOptions? options = null, CancellationToken cancellationToken = default) where TEvent : notnull;

    /// <summary>Subscribes a delegate to events of exactly type <typeparamref name="TEvent"/>. Dispose the result to unsubscribe.</summary>
    IDisposable Subscribe<TEvent>(Func<TEvent, EventContext, CancellationToken, Task> handler) where TEvent : notnull;

    /// <summary>Subscribes to raw envelopes for one event name, or every event when <paramref name="eventName"/> is null (auditing, forwarding).</summary>
    IDisposable SubscribeRaw(string? eventName, Func<EventEnvelope, CancellationToken, Task> handler);
}

/// <summary>Class-based handler resolved from DI (a fresh scope per delivery). Register with <c>AddMeshHandler</c>.</summary>
public interface IEventHandler<in TEvent> where TEvent : notnull
{
    Task HandleAsync(TEvent @event, EventContext context, CancellationToken cancellationToken);
}
