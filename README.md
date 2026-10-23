# Mesh

An internal publish/subscribe event bus. Publishers and subscribers never reference each other: one side publishes an event, the other side handles it, and the transport in between can be swapped without touching either.

Use it to let packages react to each other without direct coupling, for example Keep announcing `SecretRotated` so a cache somewhere else can drop its copy.

## Install

```bash
dotnet add package Mesh
```

## Quick start

```csharp
services.AddMesh();                                     // in-process transport
services.AddMeshHandler<SecretRotated, InvalidateCache>();    // class handler; a new DI scope per delivery

bus.Subscribe<OrderPlaced>((e, ctx, ct) => { /* ... */ return Task.CompletedTask; });   // delegate handler
await bus.PublishAsync(new OrderPlaced(id), new PublishOptions { TenantId = "t1", CorrelationId = corr });
```

## Events

- Any serializable type is an event. Give it a stable logical name with `[EventName("keep.secret-rotated")]`; without it the name is the full CLR type name. Names are the contract: subscriptions match the exact name, with no inheritance-based dispatch.
- Every event travels as an `EventEnvelope` with a JSON payload, even in-process. Behaviour is therefore identical for in-process and remote transports, and handlers never share mutable event objects.
- `EventContext` gives handlers the `EventId` (stable across redeliveries, so you can de-duplicate), `TenantId`, `CorrelationId` and `Headers`.

## Handlers

- **Delegates**: `bus.Subscribe<T>(...)`.
- **Classes**: implement `IEventHandler<T>` and register with `AddMeshHandler<TEvent, THandler>()`. Each delivery gets a fresh DI scope, so scoped services like a `DbContext` or tenant context work.
- **Raw**: `SubscribeRaw(name or null, ...)` observes raw envelopes, for audit trails or forwarding.

With the in-process transport, `PublishAsync` runs the handlers before it returns. That makes tests deterministic and supports flows like "invalidate the cache before returning".

## Failures

A handler that throws is logged and does not affect the publisher or the other handlers (`FailureMode = Log`, the default). `FailureMode = Throw` runs every handler and then throws an `AggregateException`, which is useful in tests and with transports that redeliver.

## Durable delivery on Relay

`AddMeshRelay()` (with Relay's stores, `AddRelay` and its dispatcher) makes the DI handlers durable: a publish queues one Relay message
per handler (destination `mesh:{handler type}`), so each handler is delivered, retried and dead-lettered on its own, and a failing one
never runs the others again. Delegate subscriptions stay in-process and run at once.

`IMeshOutbox.PublishAsync(connection, transaction, event)` queues the handlers inside the caller's transaction (the transactional
outbox): nothing is delivered unless it commits. Messages go to `MeshRelayOptions.Table` on that connection's database. Delivery is
at-least-once: de-duplicate by `EventContext.EventId`.

`AddMeshEventName<T>("module.name")` names an event type in code, for types that cannot carry `[EventName]`.

## Running on several instances

Implement `IEventTransport` (`Attach` and `PublishAsync`, calling `IEventReceiver.ReceiveAsync` for inbound events) and register it with `AddMeshTransport<T>()`. Durable transports are at-least-once and asynchronous, so use `EventContext.EventId` to de-duplicate.

The in-process transport and the durable Relay transport are included; Relay's Postgres stores let several instances share the work.

## Configuration

Section `Mesh`.

| Option | Default | Meaning |
|---|---|---|
| `FailureMode` | `Log` | `Log` isolates handler failures; `Throw` aggregates and rethrows them |

## Depends on

Relay (the durable transport and the transactional outbox). Tenant and correlation ids are plain strings on `PublishOptions`.
