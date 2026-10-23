using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Mesh;

internal sealed class EventBus : IEventBus, IEventReceiver
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private static readonly ConcurrentDictionary<Type, string> Names = new();

    private readonly IEventTransport _transport;
    private readonly IServiceScopeFactory _scopes;
    private readonly ILogger<EventBus> _logger;
    private readonly TimeProvider _time;
    private readonly MeshOptions _options;
    private readonly IEnumerable<HandlerRegistration> _diHandlers;
    private readonly Lock _gate = new();
    private List<Subscription> _subscriptions = [];

    public EventBus(IEventTransport transport, IServiceScopeFactory scopes, IEnumerable<HandlerRegistration> diHandlers,
        IOptions<MeshOptions> options, TimeProvider time, ILogger<EventBus> logger)
    {
        _transport = transport; _scopes = scopes; _diHandlers = diHandlers; _options = options.Value; _time = time; _logger = logger;
        transport.Attach(this);
    }

    public static string NameOf(Type t) => Names.GetOrAdd(t, static t => t.GetCustomAttribute<EventNameAttribute>()?.Name ?? t.FullName ?? t.Name);

    /// <summary>Gives a type its logical name in code (for event types the publisher does not own, so cannot mark with <see cref="EventNameAttribute"/>).</summary>
    internal static void Name(Type t, string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        var existing = Names.GetOrAdd(t, name);
        if (existing != name) throw new InvalidOperationException($"{t.FullName} is already named '{existing}'; it cannot also be '{name}'.");
    }

    /// <summary>The envelope an event travels in (also used by the durable outbox).</summary>
    internal EventEnvelope Envelope<TEvent>(TEvent @event, PublishOptions? options) where TEvent : notnull =>
        new(Guid.NewGuid(), NameOf(typeof(TEvent)), JsonSerializer.SerializeToUtf8Bytes(@event, Json), _time.GetUtcNow(), options?.CorrelationId, options?.TenantId, options?.Headers);

    /// <summary>The DI-registered handlers of an event (the durable ones: each is delivered and retried on its own).</summary>
    internal IEnumerable<HandlerRegistration> HandlersOf(string eventName) => _diHandlers.Where(h => h.EventName == eventName);

    /// <summary>Delivers to one DI handler in a fresh scope; its failure propagates, so a durable transport retries it.</summary>
    internal async Task DeliverToHandlerAsync(string handlerKey, EventEnvelope envelope, CancellationToken cancellationToken)
    {
        var registration = _diHandlers.FirstOrDefault(h => h.Key == handlerKey && h.EventName == envelope.EventName)
            ?? throw new InvalidOperationException($"No handler '{handlerKey}' for '{envelope.EventName}' is registered on this instance.");
        await using var scope = _scopes.CreateAsyncScope();
        await registration.Invoke(scope.ServiceProvider, envelope, ToContext(envelope), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Runs the delegate subscriptions only (they live in this process; durable transports deliver the DI handlers).</summary>
    internal async Task ReceiveSubscriptionsAsync(EventEnvelope envelope, CancellationToken cancellationToken)
    {
        var failures = new List<Exception>();
        foreach (var sub in Volatile.Read(ref _subscriptions))
        {
            if (sub.EventName is not null && sub.EventName != envelope.EventName) continue;
            await Guard(() => sub.Handler(envelope, cancellationToken), envelope, failures);
        }

        if (failures.Count > 0 && _options.FailureMode == HandlerFailureMode.Throw)
            throw new AggregateException($"{failures.Count} handler(s) failed for '{envelope.EventName}'.", failures);
    }

    public Task PublishAsync<TEvent>(TEvent @event, PublishOptions? options = null, CancellationToken cancellationToken = default) where TEvent : notnull
        => _transport.PublishAsync(Envelope(@event, options), cancellationToken);

    public IDisposable Subscribe<TEvent>(Func<TEvent, EventContext, CancellationToken, Task> handler) where TEvent : notnull
        => Add(new Subscription(NameOf(typeof(TEvent)), (env, ct) =>
        {
            var evt = JsonSerializer.Deserialize<TEvent>(env.Payload, Json)!;
            return handler(evt, ToContext(env), ct);
        }, this));

    public IDisposable SubscribeRaw(string? eventName, Func<EventEnvelope, CancellationToken, Task> handler)
        => Add(new Subscription(eventName, handler, this));

    public async Task ReceiveAsync(EventEnvelope envelope, CancellationToken cancellationToken)
    {
        var failures = new List<Exception>();

        foreach (var sub in Volatile.Read(ref _subscriptions))
        {
            if (sub.EventName is not null && sub.EventName != envelope.EventName) continue;
            await Guard(() => sub.Handler(envelope, cancellationToken), envelope, failures);
        }

        foreach (var reg in _diHandlers)
        {
            if (reg.EventName != envelope.EventName) continue;
            await Guard(async () =>
            {
                await using var scope = _scopes.CreateAsyncScope();
                await reg.Invoke(scope.ServiceProvider, envelope, ToContext(envelope), cancellationToken);
            }, envelope, failures);
        }

        if (failures.Count > 0 && _options.FailureMode == HandlerFailureMode.Throw)
            throw new AggregateException($"{failures.Count} handler(s) failed for '{envelope.EventName}'.", failures);
    }

    private async Task Guard(Func<Task> handler, EventEnvelope envelope, List<Exception> failures)
    {
        try { await handler().ConfigureAwait(false); }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            failures.Add(ex);
            _logger.LogError(ex, "Handler for event {EventName} ({EventId}) failed", envelope.EventName, envelope.Id);
        }
    }

    private static EventContext ToContext(EventEnvelope e)
        => new(e.Id, e.EventName, e.OccurredAt, e.CorrelationId, e.TenantId, e.Headers ?? new Dictionary<string, string>());

    private Subscription Add(Subscription s)
    {
        lock (_gate) _subscriptions = [.. _subscriptions, s];
        return s;
    }

    private void Remove(Subscription s)
    {
        lock (_gate) _subscriptions = _subscriptions.Where(x => x != s).ToList();
    }

    private sealed class Subscription(string? eventName, Func<EventEnvelope, CancellationToken, Task> handler, EventBus owner) : IDisposable
    {
        public string? EventName { get; } = eventName;
        public Func<EventEnvelope, CancellationToken, Task> Handler { get; } = handler;
        public void Dispose() => owner.Remove(this);
    }
}

/// <summary>
/// A DI-registered <see cref="IEventHandler{TEvent}"/>; <see cref="Key"/> names it for durable delivery. The event's name is read when
/// events flow, not at registration, so a handler may be registered before the event type is named.
/// </summary>
internal sealed record HandlerRegistration(Type EventType, string Key, Func<IServiceProvider, EventEnvelope, EventContext, CancellationToken, Task> Invoke)
{
    public string EventName => EventBus.NameOf(EventType);
}

/// <summary>Names of handler types that stay the same from build to build (no assembly versions), for durable delivery.</summary>
public static class HandlerKeys
{
    /// <summary><c>Namespace.Type</c>; a generic type lists its arguments the same way: <c>Ns.Adapter`2[Ns.Event,Ns.Handler]</c>.</summary>
    public static string Of(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        if (!type.IsGenericType || type.IsGenericTypeDefinition) return type.FullName ?? type.Name;
        var definition = type.GetGenericTypeDefinition();
        return $"{definition.FullName ?? definition.Name}[{string.Join(",", type.GetGenericArguments().Select(Of))}]";
    }
}
