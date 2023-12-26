namespace BitBadger.Documents.Tests;

using System;
using System.IO;
using System.Threading.Tasks;
using Sqlite;

/// <summary>
/// A throwaway SQLite database file, which will be deleted when it goes out of scope
/// </summary>
public class ThrowawaySqliteDb : IDisposable, IAsyncDisposable
{
    private readonly string _dbName;
    
    public ThrowawaySqliteDb(string dbName)
    {
        _dbName = dbName;
    }
    
    public void Dispose()
    {
        if (File.Exists(_dbName)) File.Delete(_dbName);
        GC.SuppressFinalize(this);
    }

    public ValueTask DisposeAsync()
    {
        if (File.Exists(_dbName)) File.Delete(_dbName);
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// Utility functions for dealing with SQLite databases
/// </summary>
public static class SqliteDb
{
    /// <summary>
    /// The table name for the catalog metadata
    /// </summary>
    public const string Catalog = "sqlite_master";

    /// <summary>
    /// The name of the table used for testing
    /// </summary>
    public const string TableName = "test_table";

    /// <summary>
    /// Create a throwaway database file with the test_table defined
    /// </summary>
    public static async Task<ThrowawaySqliteDb> BuildDb()
    {
        var dbName = $"test-db-{Guid.NewGuid():n}.db";
        Configuration.UseConnectionString($"data source={dbName}");
        await Definition.EnsureTable(TableName);
        return new ThrowawaySqliteDb(dbName);
    }
}
