namespace Mesh;

/// <summary>Transport-level wire format of an event: metadata plus a JSON payload.</summary>
public sealed record EventEnvelope(
    Guid Id,
    string EventName,
    byte[] Payload,
    DateTimeOffset OccurredAt,
    string? CorrelationId = null,
    string? TenantId = null,
    IReadOnlyDictionary<string, string>? Headers = null);

/// <summary>Metadata handed to handlers alongside the typed event. <see cref="EventId"/> is stable across redeliveries — use it to de-duplicate.</summary>
public sealed record EventContext(
    Guid EventId,
    string EventName,
    DateTimeOffset OccurredAt,
    string? CorrelationId,
    string? TenantId,
    IReadOnlyDictionary<string, string> Headers);

public sealed class PublishOptions
{
    public string? TenantId { get; init; }
    public string? CorrelationId { get; init; }
    public IReadOnlyDictionary<string, string>? Headers { get; init; }
}

/// <summary>Overrides the logical event name (default: the CLR type's full name). Use stable, dotted names, e.g. <c>keep.secret-rotated</c>.</summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public sealed class EventNameAttribute(string name) : Attribute
{
    public string Name { get; } = name;
}
