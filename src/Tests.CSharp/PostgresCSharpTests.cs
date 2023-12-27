using Expecto.CSharp;
using Expecto;
using BitBadger.Documents.Postgres;
using Npgsql.FSharp;

namespace BitBadger.Documents.Tests.CSharp;

using static Runner;

/// <summary>
/// C# tests for the PostgreSQL implementation of <tt>BitBadger.Documents</tt>
/// </summary>
public class PostgresCSharpTests
{
    public static Test Unit =
        TestList("Unit", new[]
        {
            TestList("Parameters", new[]
            {
                TestCase("Id succeeds", () => {
                    Expect.equal(Parameters.Id(88).Item1, "@id", "ID parameter not constructed correctly");
                }),
                TestCase("Json succeeds", () =>
                {
                    Expect.equal(Parameters.Json("@test", new { Something = "good" }).Item1, "@test",
                            "JSON parameter not constructed correctly");
                }),
                TestCase("Field succeeds", () =>
                {
                    Expect.equal(Parameters.Field(242).Item1, "@field", "Field parameter not constructed correctly");
                }),
                TestCase("None succeeds", () => {
                    Expect.isEmpty(Parameters.None, "The no-params sequence should be empty");
                })
            }),
            TestList("Query", new[]
            {
                TestList("Definition", new[]
                {
                    TestCase("EnsureTable succeeds", () =>
                    {
                        Expect.equal(Postgres.Query.Definition.EnsureTable(PostgresDb.TableName),
                            $"CREATE TABLE IF NOT EXISTS {PostgresDb.TableName} (data JSONB NOT NULL)",
                            "CREATE TABLE statement not constructed correctly");
                    }),
                    TestCase("EnsureJsonIndex succeeds for full index", () =>
                    {
                        Expect.equal(Postgres.Query.Definition.EnsureJsonIndex("schema.tbl", DocumentIndex.Full),
                            "CREATE INDEX IF NOT EXISTS idx_tbl_document ON schema.tbl USING GIN (data)",
                            "CREATE INDEX statement not constructed correctly");
                    }),
                    TestCase("EnsureJsonIndex succeeds for JSONB Path Ops index", () =>
                    {
                        Expect.equal(
                            Postgres.Query.Definition.EnsureJsonIndex(PostgresDb.TableName, DocumentIndex.Optimized),
                            string.Format(
                                "CREATE INDEX IF NOT EXISTS idx_{0}_document ON {0} USING GIN (data jsonb_path_ops)",
                                PostgresDb.TableName),
                            "CREATE INDEX statement not constructed correctly");
                    })
                }),
                TestCase("WhereDataContains succeeds", () =>
                {
                    Expect.equal(Postgres.Query.WhereDataContains("@test"), "data @> @test",
                        "WHERE clause not correct");
                }),
                TestCase("WhereJsonPathMatches succeeds", () =>
                {
                    Expect.equal(Postgres.Query.WhereJsonPathMatches("@path"), "data @? @path::jsonpath",
                        "WHERE clause not correct");
                }),
                TestList("Count", new[]
                {
                    TestCase("ByContains succeeds", () =>
                    {
                        Expect.equal(Postgres.Query.Count.ByContains(PostgresDb.TableName),
                            $"SELECT COUNT(*) AS it FROM {PostgresDb.TableName} WHERE data @> @criteria",
                            "JSON containment count query not correct");
                    }),
                    TestCase("ByJsonPath succeeds", () =>
                    {
                        Expect.equal(Postgres.Query.Count.ByJsonPath(PostgresDb.TableName),
                            $"SELECT COUNT(*) AS it FROM {PostgresDb.TableName} WHERE data @? @path::jsonpath",
                            "JSON Path match count query not correct");
                    })
                }),
                TestList("Exists", new[]
                {
                    TestCase("ByContains succeeds", () =>
                    {
                        Expect.equal(Postgres.Query.Exists.ByContains(PostgresDb.TableName),
                            $"SELECT EXISTS (SELECT 1 FROM {PostgresDb.TableName} WHERE data @> @criteria) AS it",
                            "JSON containment exists query not correct");
                    }),
                    TestCase("byJsonPath succeeds", () =>
                    {
                        Expect.equal(Postgres.Query.Exists.ByJsonPath(PostgresDb.TableName),
                            $"SELECT EXISTS (SELECT 1 FROM {PostgresDb.TableName} WHERE data @? @path::jsonpath) AS it",
                            "JSON Path match existence query not correct");
                    })
                }),
                TestList("Find", new[]
                {
                    TestCase("byContains succeeds", () =>
                    {
                        Expect.equal(Postgres.Query.Find.ByContains(PostgresDb.TableName),
                            $"SELECT data FROM {PostgresDb.TableName} WHERE data @> @criteria",
                            "SELECT by JSON containment query not correct");
                    }),
                    TestCase("byJsonPath succeeds", () =>
                    {
                        Expect.equal(Postgres.Query.Find.ByJsonPath(PostgresDb.TableName),
                            $"SELECT data FROM {PostgresDb.TableName} WHERE data @? @path::jsonpath",
                            "SELECT by JSON Path match query not correct");
                    })
                }),
                TestList("Update", new[]
                {
                    TestCase("partialById succeeds", () =>
                    {
                        Expect.equal(Postgres.Query.Update.PartialById(PostgresDb.TableName),
                            $"UPDATE {PostgresDb.TableName} SET data = data || @data WHERE data ->> 'Id' = @id",
                            "UPDATE partial by ID statement not correct");
                    }),
                    TestCase("partialByField succeeds", () =>
                    {
                        Expect.equal(Postgres.Query.Update.PartialByField(PostgresDb.TableName, "Snail", Op.LT),
                            $"UPDATE {PostgresDb.TableName} SET data = data || @data WHERE data ->> 'Snail' < @field",
                            "UPDATE partial by ID statement not correct");
                    }),
                    TestCase("partialByContains succeeds", () =>
                    {
                        Expect.equal(Postgres.Query.Update.PartialByContains(PostgresDb.TableName),
                            $"UPDATE {PostgresDb.TableName} SET data = data || @data WHERE data @> @criteria",
                            "UPDATE partial by JSON containment statement not correct");
                    }),
                    TestCase("partialByJsonPath succeeds", () =>
                    {
                        Expect.equal(Postgres.Query.Update.PartialByJsonPath(PostgresDb.TableName),
                            $"UPDATE {PostgresDb.TableName} SET data = data || @data WHERE data @? @path::jsonpath",
                            "UPDATE partial by JSON Path statement not correct");
                    })
                }),
                TestList("Delete", new[]
                {
                    TestCase("byContains succeeds", () =>
                    {
                        Expect.equal(Postgres.Query.Delete.ByContains(PostgresDb.TableName),
                            $"DELETE FROM {PostgresDb.TableName} WHERE data @> @criteria",
                            "DELETE by JSON containment query not correct");
                    }),
                    TestCase("byJsonPath succeeds", () =>
                    {
                        Expect.equal(Postgres.Query.Delete.ByJsonPath(PostgresDb.TableName),
                            $"DELETE FROM {PostgresDb.TableName} WHERE data @? @path::jsonpath",
                            "DELETE by JSON Path match query not correct");
                    })
                })
            })
        });

    private static readonly List<JsonDocument> TestDocuments = new()
    {
        new() { Id = "one", Value = "FIRST!", NumValue = 0 },
        new() { Id = "two", Value = "another", NumValue = 10, Sub = new() { Foo = "green", Bar = "blue" } },
        new() { Id = "three", Value = "", NumValue = 4 },
        new() { Id = "four", Value = "purple", NumValue = 17, Sub = new() { Foo = "green", Bar = "red" } },
        new() { Id = "five", Value = "purple", NumValue = 18 }
    };

    internal static async Task LoadDocs()
    {
        foreach (var doc in TestDocuments) await Document.Insert(SqliteDb.TableName, doc);
    }

    /// <summary>
    /// All Postgres C# tests
    /// </summary>
    public static Test All = TestList("Postgres.C#", new[] { Unit });
}
