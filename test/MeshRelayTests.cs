using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Relay;
using Testcontainers.PostgreSql;

namespace Mesh.Tests;

public sealed record InvoiceIssued(string Number);

public sealed class Books(Log log) : IEventHandler<InvoiceIssued>
{
    public Task HandleAsync(InvoiceIssued e, EventContext c, CancellationToken ct) { log.Items.Add($"books:{e.Number}:{c.TenantId}"); return Task.CompletedTask; }
}

/// <summary>Fails the first time it sees an event, then succeeds.</summary>
public sealed class FlakyMailer(Log log) : IEventHandler<InvoiceIssued>
{
    private static readonly HashSet<Guid> Seen = [];

    public Task HandleAsync(InvoiceIssued e, EventContext c, CancellationToken ct)
    {
        lock (Seen)
        {
            if (Seen.Add(c.EventId)) throw new InvalidOperationException("mail server down");
        }

        log.Items.Add($"mail:{e.Number}");
        return Task.CompletedTask;
    }
}

public sealed class PgFixture : IAsyncLifetime
{
    private PostgreSqlContainer? _c;
    public bool Available { get; private set; }
    public string Reason { get; private set; } = "Docker/Postgres unavailable";
    public NpgsqlDataSource? DataSource { get; private set; }

    public async ValueTask InitializeAsync()
    {
        try
        {
            _c = new PostgreSqlBuilder("postgres:17-alpine").Build();
            await _c.StartAsync();
            DataSource = NpgsqlDataSource.Create(_c.GetConnectionString());
            await using (var cmd = DataSource.CreateCommand("CREATE SCHEMA events; CREATE TABLE invoices (number text)")) await cmd.ExecuteNonQueryAsync();
            await new PostgresRelayStore(DataSource, "events.outbox").EnsureSchemaAsync();
            Available = true;
        }
        catch (Exception ex) { Reason = "Docker/Postgres unavailable: " + ex.GetType().Name; }
    }

    public async ValueTask DisposeAsync()
    {
        if (DataSource is not null) await DataSource.DisposeAsync();
        if (_c is not null) await _c.DisposeAsync();
    }
}

public class MeshRelayTests(PgFixture pg) : IClassFixture<PgFixture>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static ServiceProvider Build(Action<IServiceCollection>? stores = null)
    {
        var s = new ServiceCollection();
        s.AddLogging();
        s.AddSingleton<Log>();
        stores?.Invoke(s);
        s.AddRelay(o => { o.BaseDelay = TimeSpan.Zero; o.MaxDelay = TimeSpan.Zero; });
        s.AddMeshRelay(o => o.Table = "events.outbox");
        s.AddMeshEventName<InvoiceIssued>("sales.invoice_issued");
        s.AddMeshHandler<InvoiceIssued, Books>();
        s.AddMeshHandler<InvoiceIssued, FlakyMailer>();
        return s.BuildServiceProvider();
    }

    [Fact]
    public async Task Each_handler_is_delivered_and_retried_on_its_own()
    {
        await using var sp = Build();
        var bus = sp.GetRequiredService<IEventBus>();
        var log = sp.GetRequiredService<Log>();
        var seen = new List<string>();
        bus.Subscribe<InvoiceIssued>((e, c, _) => { seen.Add(c.EventName); return Task.CompletedTask; });

        await bus.PublishAsync(new InvoiceIssued("A-1"), new PublishOptions { TenantId = "t1" }, Ct);
        Assert.Equal(["sales.invoice_issued"], seen); // delegate subscriptions run at once, in this process
        Assert.Empty(log.Items);                       // DI handlers wait for Relay

        var relay = sp.GetRequiredService<IRelayProcessor>();
        await relay.ProcessOnceAsync(Ct);              // the books take it; the mailer fails and is retried
        Assert.Equal(["books:A-1:t1"], log.Items);
        await relay.ProcessOnceAsync(Ct);
        Assert.Equal(["books:A-1:t1", "mail:A-1"], log.Items); // only the failed handler ran again
        Assert.Equal(0, (await sp.GetRequiredService<IRelayAdmin>().GetStatsAsync(Ct)).Pending);
    }

    [Fact]
    public async Task Events_published_in_a_transaction_are_delivered_only_if_it_commits()
    {
        Assert.SkipUnless(pg.Available, pg.Reason);
        await using var sp = Build(s => s.AddRelayPostgresStore(pg.DataSource!, "events.outbox"));
        var outbox = sp.GetRequiredService<IMeshOutbox>();
        var log = sp.GetRequiredService<Log>();

        await using (var connection = await pg.DataSource!.OpenConnectionAsync(Ct))
        {
            await using (var rolledBack = await connection.BeginTransactionAsync(Ct))
            {
                await outbox.PublishAsync(connection, rolledBack, new InvoiceIssued("B-1"), new PublishOptions { TenantId = "t2" }, Ct);
            }

            await using var committed = await connection.BeginTransactionAsync(Ct);
            await using (var insert = new NpgsqlCommand("INSERT INTO invoices VALUES ('B-2')", connection, committed)) await insert.ExecuteNonQueryAsync(Ct);
            await outbox.PublishAsync(connection, committed, new InvoiceIssued("B-2"), new PublishOptions { TenantId = "t2" }, Ct);
            await committed.CommitAsync(Ct);
        }

        var relay = sp.GetRequiredService<IRelayProcessor>();
        await relay.ProcessOnceAsync(Ct);
        await relay.ProcessOnceAsync(Ct);
        Assert.Equal(["books:B-2:t2", "mail:B-2"], log.Items);
    }
}
