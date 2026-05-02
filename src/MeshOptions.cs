namespace Mesh;

public enum HandlerFailureMode
{
    /// <summary>Log and continue; the publisher never sees subscriber failures (default — decoupling).</summary>
    Log,
    /// <summary>Run every handler, then throw an <see cref="AggregateException"/> from the receive path (useful in tests and for transports that redeliver).</summary>
    Throw,
}

public sealed class MeshOptions
{
    public const string SectionName = "Mesh";
    public HandlerFailureMode FailureMode { get; set; } = HandlerFailureMode.Log;
}
