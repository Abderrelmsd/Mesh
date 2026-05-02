namespace Mesh;

/// <summary>Receives envelopes coming off a transport and dispatches them to local subscribers. Implemented by the bus.</summary>
public interface IEventReceiver
{
    Task ReceiveAsync(EventEnvelope envelope, CancellationToken cancellationToken);
}

/// <summary>
/// Moves envelopes between bus instances. Swap the implementation (Postgres outbox, broker…) without touching
/// publishers or subscribers. A transport must eventually call <see cref="IEventReceiver.ReceiveAsync"/> for every
/// envelope that should reach this instance — including ones this instance published, if it has local subscribers.
/// </summary>
public interface IEventTransport
{
    /// <summary>Called once by the bus at construction.</summary>
    void Attach(IEventReceiver receiver);

    Task PublishAsync(EventEnvelope envelope, CancellationToken cancellationToken);
}

/// <summary>Default transport: delivers straight back to local subscribers, synchronously with the publish.</summary>
public sealed class InProcessTransport : IEventTransport
{
    private IEventReceiver? _receiver;

    public void Attach(IEventReceiver receiver) => _receiver = receiver;

    public Task PublishAsync(EventEnvelope envelope, CancellationToken cancellationToken)
        => _receiver?.ReceiveAsync(envelope, cancellationToken) ?? Task.CompletedTask;
}
