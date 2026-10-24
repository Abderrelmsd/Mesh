using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Mesh;

public static class ServiceCollectionExtensions
{
    /// <summary>Registers <see cref="IEventBus"/> with the in-process transport unless an <see cref="IEventTransport"/> is already registered.</summary>
    public static IServiceCollection AddMesh(this IServiceCollection services, Action<MeshOptions>? configure = null)
    {
        var builder = services.AddOptions<MeshOptions>();
        if (configure is not null) builder.Configure(configure);
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IEventTransport, InProcessTransport>();
        services.TryAddSingleton<EventBus>();
        services.TryAddSingleton<IEventBus>(sp => sp.GetRequiredService<EventBus>());
        return services;
    }

    /// <summary>Replaces the transport (call before or after <see cref="AddMesh"/>).</summary>
    public static IServiceCollection AddMeshTransport<TTransport>(this IServiceCollection services) where TTransport : class, IEventTransport
    {
        services.RemoveAll<IEventTransport>();
        return services.AddSingleton<IEventTransport, TTransport>();
    }

    /// <summary>Names an event type in code, like <see cref="EventNameAttribute"/> (call before registering its handlers).</summary>
    public static IServiceCollection AddMeshEventName<TEvent>(this IServiceCollection services, string name) where TEvent : notnull
    {
        EventBus.Name(typeof(TEvent), name);
        return services;
    }

    /// <summary>Registers a DI-resolved handler; a new scope is created per delivery. Its durable key is <see cref="HandlerKeys.Of"/> its type.</summary>
    public static IServiceCollection AddMeshHandler<TEvent, THandler>(this IServiceCollection services)
        where TEvent : notnull where THandler : class, IEventHandler<TEvent>
        => services.AddMeshHandler<TEvent, THandler>(HandlerKeys.Of(typeof(THandler)));

    /// <summary>
    /// Registers a DI-resolved handler under an explicit durable key (messages already queued for a key keep finding the handler when
    /// its type is renamed or moved). Keys are unique per event.
    /// </summary>
    public static IServiceCollection AddMeshHandler<TEvent, THandler>(this IServiceCollection services, string key)
        where TEvent : notnull where THandler : class, IEventHandler<TEvent>
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        services.TryAddScoped<THandler>();
        services.AddSingleton(new HandlerRegistration(typeof(TEvent), key, (sp, env, ctx, ct) =>
        {
            var evt = JsonSerializer.Deserialize<TEvent>(env.Payload, new JsonSerializerOptions(JsonSerializerDefaults.Web))!;
            return sp.GetRequiredService<THandler>().HandleAsync(evt, ctx, ct);
        }));
        return services;
    }
}
