using BitBadger.Documents.Postgres;
using Npgsql;
using Npgsql.FSharp;
using ThrowawayDb.Postgres;

namespace BitBadger.Documents.Tests;

/// <summary>
/// A throwaway SQLite database file, which will be deleted when it goes out of scope
/// </summary>
public class ThrowawayPostgresDb : IDisposable, IAsyncDisposable
{
    private readonly ThrowawayDatabase _db;

    public string ConnectionString => _db.ConnectionString;
    
    public ThrowawayPostgresDb(ThrowawayDatabase db)
    {
        _db = db;
    }
    
    public void Dispose()
    {
        _db.Dispose();
        GC.SuppressFinalize(this);
    }

    public ValueTask DisposeAsync()
    {
        _db.Dispose();
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// Database helpers for PostgreSQL integration tests
/// </summary>
public static class PostgresDb
{
    /// <summary>
    /// The name of the table used for testing
    /// </summary>
    public const string TableName = "test_table";

    /// <summary>
    /// The host for the database
    /// </summary>
    private static readonly Lazy<string> DbHost = new(() =>
    {
        return Environment.GetEnvironmentVariable("BitBadger.Documents.Postgres.DbHost") switch
        {
            null => "localhost",
            var host when host.Trim() == "" => "localhost",
            var host => host
        };
    });

    /// <summary>
    /// The port for the database
    /// </summary>
    private static readonly Lazy<int> DbPort = new(() =>
    {
        return Environment.GetEnvironmentVariable("BitBadger.Documents.Postgres.DbPort") switch
        {
            null => 5432,
            var port when port.Trim() == "" => 5432,
            var port => int.Parse(port)
        };
    });

    /// <summary>
    /// The database itself
    /// </summary>
    private static readonly Lazy<string> DbDatabase = new(() =>
    {
        return Environment.GetEnvironmentVariable("BitBadger.Documents.Postres.DbDatabase") switch
        {
            null => "postgres",
            var db when db.Trim() == "" => "postgres",
            var db => db
        };
    });

    /// <summary>
    /// The user to use in connecting to the database
    /// </summary>
    private static readonly Lazy<string> DbUser = new(() =>
    {
        return Environment.GetEnvironmentVariable("BitBadger.Documents.Postgres.DbUser") switch
        {
            null => "postgres",
            var user when user.Trim() == "" => "postgres",
            var user => user
        };
    });

    /// <summary>
    /// The password to use for the database
    /// </summary>
    private static readonly Lazy<string> DbPassword = new(() =>
    {
        return Environment.GetEnvironmentVariable("BitBadger.Documents.Postrgres.DbPwd") switch
        {
            null => "postgres",
            var pwd when pwd.Trim() == "" => "postgres",
            var pwd => pwd
        };
    });

    /// <summary>
    /// The overall connection string
    /// </summary>
    public static readonly Lazy<string> ConnStr = new(() =>
        Sql.formatConnectionString(
            Sql.password(DbPassword.Value,
                Sql.username(DbUser.Value,
                    Sql.database(DbDatabase.Value,
                        Sql.port(DbPort.Value,
                            Sql.host(DbHost.Value)))))));

    /// <summary>
    /// Create a data source using the derived connection string
    /// </summary>
    public static NpgsqlDataSource MkDataSource(string cStr) =>
        new NpgsqlDataSourceBuilder(cStr).Build();

    /// <summary>
    /// Build the throwaway database
    /// </summary>
    public static ThrowawayPostgresDb BuildDb()
    {
        var database = ThrowawayDatabase.Create(ConnStr.Value);

        var sqlProps = Sql.connect(database.ConnectionString);

        Sql.executeNonQuery(Sql.query(Postgres.Query.Definition.EnsureTable(TableName), sqlProps));
        Sql.executeNonQuery(Sql.query(Query.Definition.EnsureKey(TableName), sqlProps));

        Postgres.Configuration.useDataSource(MkDataSource(database.ConnectionString));

        return new ThrowawayPostgresDb(database);
    }
}
