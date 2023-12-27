using Expecto.CSharp;
using Expecto;
using BitBadger.Documents.Postgres;
using Npgsql;

namespace BitBadger.Documents.Tests.CSharp;

using static CommonExtensionsAndTypesForNpgsqlFSharp;
using static Runner;

/// <summary>
/// C# tests for the extensions on the <tt>NpgsqlConnection</tt> type
/// </summary>
public class PostgresCSharpExtensionTests
{
    private static Task LoadDocs() => PostgresCSharpTests.LoadDocs();

    /// <summary>
    /// Create a connection to the throwaway database
    /// </summary>
    /// <param name="db">The throwaway database for which a connection should be made</param>
    /// <returns>An open connection to the throwaway database</returns>
    private static NpgsqlConnection MkConn(ThrowawayPostgresDb db)
    {
        var conn = new NpgsqlConnection(db.ConnectionString);
        conn.Open();
        return conn;
    }

    /// <summary>
    /// Integration tests for the SQLite extension methods
    /// </summary>
    [Tests]
    public static readonly Test Integration = TestList("Postgres.C#.Extensions", new[]
    {
        TestList("CustomList", new[]
        {
            TestCase("succeeds when data is found", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var docs = await conn.CustomList(Query.SelectFromTable(PostgresDb.TableName), Parameters.None,
                    Results.FromData<JsonDocument>);
                Expect.equal(docs.Count, 5, "There should have been 5 documents returned");
            }),
            TestCase("succeeds when data is not found", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var docs = await conn.CustomList(
                    $"SELECT data FROM {PostgresDb.TableName} WHERE data @? @path::jsonpath",
                    new[] { Tuple.Create("@path", Sql.@string("$.NumValue ? (@ > 100)")) },
                    Results.FromData<JsonDocument>);
                Expect.isEmpty(docs, "There should have been no documents returned");
            })
        }),
        TestList("CustomSingle", new[]
        {
            TestCase("succeeds when a row is found", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var doc = await conn.CustomSingle($"SELECT data FROM {PostgresDb.TableName} WHERE data ->> 'Id' = @id",
                    new[] { Tuple.Create("@id", Sql.@string("one")) }, Results.FromData<JsonDocument>);
                Expect.isNotNull(doc, "There should have been a document returned");
                Expect.equal(doc.Id, "one", "The incorrect document was returned");
            }),
            TestCase("succeeds when a row is not found", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var doc = await conn.CustomSingle($"SELECT data FROM {PostgresDb.TableName} WHERE data ->> 'Id' = @id",
                    new[] { Tuple.Create("@id", Sql.@string("eighty")) }, Results.FromData<JsonDocument>);
                Expect.isNull(doc, "There should not have been a document returned");
            })
        }),
        TestList("CustomNonQuery", new[]
        {
            TestCase("succeeds when operating on data", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                await conn.CustomNonQuery($"DELETE FROM {PostgresDb.TableName}", Parameters.None);
        
                var remaining = await conn.CountAll(PostgresDb.TableName);
                Expect.equal(remaining, 0, "There should be no documents remaining in the table");
            }),
            TestCase("succeeds when no data matches where clause", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                await conn.CustomNonQuery($"DELETE FROM {PostgresDb.TableName} WHERE data @? @path::jsonpath",
                    new[] { Tuple.Create("@path", Sql.@string("$.NumValue ? (@ > 100)")) });
        
                var remaining = await conn.CountAll(PostgresDb.TableName);
                Expect.equal(remaining, 5, "There should be 5 documents remaining in the table");
            })
        }),
        TestCase("Scalar succeeds", async () =>
        {
            await using var db = PostgresDb.BuildDb();
            await using var conn = MkConn(db);
            var nbr = await conn.CustomScalar("SELECT 5 AS test_value", Parameters.None, row => row.@int("test_value"));
            Expect.equal(nbr, 5, "The query should have returned the number 5");
        }),
        TestCase("EnsureTable succeeds", async () =>
        {
            await using var db = PostgresDb.BuildDb();
            await using var conn = MkConn(db);
            var tableExists = () => conn.CustomScalar(
                "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'ensured') AS it", Parameters.None,
                Results.ToExists);
            var keyExists = () => conn.CustomScalar(
                "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'idx_ensured_key') AS it", Parameters.None,
                Results.ToExists);

            var exists = await tableExists();
            var alsoExists = await keyExists();
            Expect.isFalse(exists, "The table should not exist already");
            Expect.isFalse(alsoExists, "The key index should not exist already");

            await conn.EnsureTable("ensured");
            exists = await tableExists();
            alsoExists = await keyExists();
            Expect.isTrue(exists, "The table should now exist");
            Expect.isTrue(alsoExists, "The key index should now exist");
        }),
        TestCase("EnsureDocumentIndex succeeds", async () =>
        {
            await using var db = PostgresDb.BuildDb();
            await using var conn = MkConn(db);
            var indexExists = () => conn.CustomScalar(
                "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'idx_ensured_document') AS it", Parameters.None,
                Results.ToExists);

            var exists = await indexExists();
            Expect.isFalse(exists, "The index should not exist already");

            await conn.EnsureTable("ensured");
            await conn.EnsureDocumentIndex("ensured", DocumentIndex.Optimized);
            exists = await indexExists();
            Expect.isTrue(exists, "The index should now exist");
        }),
        TestCase("EnsureFieldIndex succeeds", async () =>
        {
            await using var db = PostgresDb.BuildDb();
            await using var conn = MkConn(db);
            var indexExists = () => conn.CustomScalar(
                "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'idx_ensured_test') AS it", Parameters.None,
                Results.ToExists);

            var exists = await indexExists();
            Expect.isFalse(exists, "The index should not exist already");

            await conn.EnsureTable("ensured");
            await conn.EnsureFieldIndex("ensured", "test", new[] { "Id", "Category" });
            exists = await indexExists();
            Expect.isTrue(exists, "The index should now exist");
        }),
        TestList("Insert", new[]
        {
            TestCase("succeeds", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                var before = await conn.CountAll(PostgresDb.TableName);
                Expect.equal(before, 0, "There should be no documents in the table");
        
                await conn.Insert(PostgresDb.TableName,
                    new JsonDocument { Id = "turkey", Sub = new() { Foo = "gobble", Bar = "gobble" } });
                var after = await conn.FindAll<JsonDocument>(PostgresDb.TableName);
                Expect.equal(after.Count, 1, "There should have been one document inserted");
            }),
            TestCase("fails for duplicate key", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await conn.Insert(PostgresDb.TableName, new JsonDocument { Id = "test" });
                try
                {
                    await conn.Insert(PostgresDb.TableName, new JsonDocument { Id = "test" });
                    Expect.isTrue(false, "An exception should have been raised for duplicate document ID insert");
                }
                catch (Exception)
                {
                    // This is what should have happened
                }
            })
        }),
        TestList("save", new[]
        {
            TestCase("succeeds when a document is inserted", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                var before = await conn.CountAll(PostgresDb.TableName);
                Expect.equal(before, 0, "There should be no documents in the table");
        
                await conn.Save(PostgresDb.TableName,
                    new JsonDocument { Id = "test", Sub = new() { Foo = "a", Bar = "b" } });
                var after = await conn.FindAll<JsonDocument>(PostgresDb.TableName);
                Expect.equal(after.Count, 1, "There should have been one document inserted");
            }),
            TestCase("succeeds when a document is updated", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await conn.Insert(PostgresDb.TableName,
                    new JsonDocument { Id = "test", Sub = new() { Foo = "a", Bar = "b" } });

                var before = await conn.FindById<string, JsonDocument>(PostgresDb.TableName, "test");
                Expect.isNotNull(before, "There should have been a document returned");
                Expect.equal(before.Id, "test", "The document is not correct");
        
                await conn.Save(PostgresDb.TableName,
                    new JsonDocument { Id = "test", Sub = new() { Foo = "c", Bar = "d" } });
                var after = await conn.FindById<string, JsonDocument>(PostgresDb.TableName, "test");
                Expect.isNotNull(after, "There should have been a document returned post-update");
                Expect.equal(after.Sub!.Foo, "c", "The updated document is not correct");
            })
        }),
        TestCase("CountAll succeeds", async () =>
        {
            await using var db = PostgresDb.BuildDb();
            await using var conn = MkConn(db);
            await LoadDocs();
        
            var theCount = await conn.CountAll(PostgresDb.TableName);
            Expect.equal(theCount, 5, "There should have been 5 matching documents");
        }),
        TestCase("CountByField succeeds", async () =>
        {
            await using var db = PostgresDb.BuildDb();
            await using var conn = MkConn(db);
            await LoadDocs();

            var theCount = await conn.CountByField(PostgresDb.TableName, "Value", Op.EQ, "purple");
            Expect.equal(theCount, 2, "There should have been 2 matching documents");
        }),
        TestCase("CountByContains succeeds", async () =>
        {
            await using var db = PostgresDb.BuildDb();
            await using var conn = MkConn(db);
            await LoadDocs();

            var theCount = await conn.CountByContains(PostgresDb.TableName, new { Value = "purple" });
            Expect.equal(theCount, 2, "There should have been 2 matching documents");
        }),
        TestCase("CountByJsonPath succeeds", async () =>
        {
            await using var db = PostgresDb.BuildDb();
            await using var conn = MkConn(db);
            await LoadDocs();

            var theCount = await conn.CountByJsonPath(PostgresDb.TableName, "$.NumValue ? (@ > 5)");
            Expect.equal(theCount, 3, "There should have been 3 matching documents");
        }),
        TestList("ExistsById", new[]
        {
            TestCase("succeeds when a document exists", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var exists = await conn.ExistsById(PostgresDb.TableName, "three");
                Expect.isTrue(exists, "There should have been an existing document");
            }),
            TestCase("succeeds when a document does not exist", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var exists = await conn.ExistsById(PostgresDb.TableName, "seven");
                Expect.isFalse(exists, "There should not have been an existing document");
            })
        }),
        TestList("ExistsByField", new[]
        {
            TestCase("succeeds when documents exist", async () =>
            {
                await using var db   = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var exists = await conn.ExistsByField(PostgresDb.TableName, "Sub", Op.EX, "");
                Expect.isTrue(exists, "There should have been existing documents");
            }),
            TestCase("succeeds when documents do not exist", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var exists = await conn.ExistsByField(PostgresDb.TableName, "NumValue", Op.EQ, "six");
                Expect.isFalse(exists, "There should not have been existing documents");
            })
        }),
        TestList("ExistsByContains", new[]
        {
            TestCase("succeeds when documents exist", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var exists = await conn.ExistsByContains(PostgresDb.TableName, new { NumValue = 10 });
                Expect.isTrue(exists, "There should have been existing documents");
            }),
            TestCase("succeeds when no matching documents exist", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var exists = await conn.ExistsByContains(PostgresDb.TableName, new { Nothing = "none" });
                Expect.isFalse(exists, "There should not have been any existing documents");
            })
        }),
        TestList("ExistsByJsonPath", new[]
        {
            TestCase("succeeds when documents exist", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var exists = await conn.ExistsByJsonPath(PostgresDb.TableName, "$.Sub.Foo ? (@ == \"green\")");
                Expect.isTrue(exists, "There should have been existing documents");
            }),
            TestCase("succeeds when no matching documents exist", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var exists = await conn.ExistsByJsonPath(PostgresDb.TableName, "$.NumValue ? (@ > 1000)");
                Expect.isFalse(exists, "There should not have been any existing documents");
            })
        }),
        TestList("FindAll", new[]
        {
            TestCase("succeeds when there is data", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);

                await conn.Insert(PostgresDb.TableName, new JsonDocument { Id = "one" });
                await conn.Insert(PostgresDb.TableName, new JsonDocument { Id = "three" });
                await conn.Insert(PostgresDb.TableName, new JsonDocument { Id = "five" });

                var results = await conn.FindAll<JsonDocument>(PostgresDb.TableName);
                Expect.equal(results.Count, 3, "There should have been 3 documents returned");
            }),
            TestCase("succeeds when there is no data", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                var results = await conn.FindAll<JsonDocument>(PostgresDb.TableName);
                Expect.isEmpty(results, "There should have been no documents returned");
            })
        }),
        TestList("FindById", new[]
        {
            TestCase("succeeds when a document is found", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var doc = await conn.FindById<string, JsonDocument>(PostgresDb.TableName, "two");
                Expect.isNotNull(doc, "There should have been a document returned");
                Expect.equal(doc.Id, "two", "The incorrect document was returned");
            }),
            TestCase("succeeds when a document is not found", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var doc = await conn.FindById<string, JsonDocument>(PostgresDb.TableName, "three hundred eighty-seven");
                Expect.isNull(doc, "There should not have been a document returned");
            })
        }),
        TestList("FindByField", new[]
        {
            TestCase("succeeds when documents are found", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var docs = await conn.FindByField<JsonDocument>(PostgresDb.TableName, "Value", Op.EQ, "another");
                Expect.equal(docs.Count, 1, "There should have been one document returned");
            }),
            TestCase("succeeds when documents are not found", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var docs = await conn.FindByField<JsonDocument>(PostgresDb.TableName, "Value", Op.EQ, "mauve");
                Expect.isEmpty(docs, "There should have been no documents returned");
            })
        }),
        TestList("FindByContains", new[]
        {
            TestCase("succeeds when documents are found", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var docs = await conn.FindByContains<JsonDocument>(PostgresDb.TableName,
                    new { Sub = new { Foo = "green" } });
                Expect.equal(docs.Count, 2, "There should have been two documents returned");
            }),
            TestCase("succeeds when documents are not found", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var docs = await conn.FindByContains<JsonDocument>(PostgresDb.TableName, new { Value = "mauve" });
                Expect.isEmpty(docs, "There should have been no documents returned");
            })
        }),
        TestList("FindByJsonPath", new[]
        {
            TestCase("succeeds when documents are found", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var docs = await conn.FindByJsonPath<JsonDocument>(PostgresDb.TableName, "$.NumValue ? (@ < 15)");
                Expect.equal(docs.Count, 3, "There should have been 3 documents returned");
            }),
            TestCase("succeeds when documents are not found", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var docs = await conn.FindByJsonPath<JsonDocument>(PostgresDb.TableName, "$.NumValue ? (@ < 0)");
                Expect.isEmpty(docs, "There should have been no documents returned");
            })
        }),
        TestList("FindFirstByField", new[]
        {
            TestCase("succeeds when a document is found", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var doc = await conn.FindFirstByField<JsonDocument>(PostgresDb.TableName, "Value", Op.EQ, "another");
                Expect.isNotNull(doc, "There should have been a document returned");
                Expect.equal(doc.Id, "two", "The incorrect document was returned");
            }),
            TestCase("succeeds when multiple documents are found", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var doc = await conn.FindFirstByField<JsonDocument>(PostgresDb.TableName, "Value", Op.EQ, "purple");
                Expect.isNotNull(doc, "There should have been a document returned");
                Expect.contains(new[] { "five", "four" }, doc.Id, "An incorrect document was returned");
            }),
            TestCase("succeeds when a document is not found", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var doc = await conn.FindFirstByField<JsonDocument>(PostgresDb.TableName, "Value", Op.EQ, "absent");
                Expect.isNull(doc, "There should not have been a document returned");
            })
        }),
        TestList("FindFirstByContains", new[]
        {
            TestCase("succeeds when a document is found", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var doc = await conn.FindFirstByContains<JsonDocument>(PostgresDb.TableName, new { Value = "another" });
                Expect.isNotNull(doc, "There should have been a document returned");
                Expect.equal(doc.Id, "two", "The incorrect document was returned");
            }),
            TestCase("succeeds when multiple documents are found", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var doc = await conn.FindFirstByContains<JsonDocument>(PostgresDb.TableName,
                    new { Sub = new { Foo = "green" } });
                Expect.isNotNull(doc, "There should have been a document returned");
                Expect.contains(new[] { "two", "four" }, doc.Id, "An incorrect document was returned");
            }),
            TestCase("succeeds when a document is not found", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var doc = await conn.FindFirstByContains<JsonDocument>(PostgresDb.TableName, new { Value = "absent" });
                Expect.isNull(doc, "There should not have been a document returned");
            })
        }),
        TestList("FindFirstByJsonPath", new[]
        {
            TestCase("succeeds when a document is found", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var doc = await conn.FindFirstByJsonPath<JsonDocument>(PostgresDb.TableName,
                    "$.Value ? (@ == \"FIRST!\")");
                Expect.isNotNull(doc, "There should have been a document returned");
                Expect.equal(doc.Id, "one", "The incorrect document was returned");
            }),
            TestCase("succeeds when multiple documents are found", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var doc = await conn.FindFirstByJsonPath<JsonDocument>(PostgresDb.TableName,
                    "$.Sub.Foo ? (@ == \"green\")");
                Expect.isNotNull(doc, "There should have been a document returned");
                Expect.contains(new[] { "two", "four" }, doc.Id, "An incorrect document was returned");
            }),
            TestCase("succeeds when a document is not found", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                var doc = await conn.FindFirstByJsonPath<JsonDocument>(PostgresDb.TableName, "$.Id ? (@ == \"nope\")");
                Expect.isNull(doc, "There should not have been a document returned");
            })
        }),
        TestList("UpdateFull", new[]
        {
            TestCase("succeeds when a document is updated", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                await conn.UpdateFull(PostgresDb.TableName, "one",
                    new JsonDocument { Id = "one", Sub = new() { Foo = "blue", Bar = "red" } });
                var after = await conn.FindById<string, JsonDocument>(PostgresDb.TableName, "one");
                Expect.isNotNull(after, "There should have been a document returned post-update");
                Expect.equal(after.Id, "one", "The updated document is not correct (ID)");
                Expect.equal(after.Value, "", "The updated document is not correct (Value)");
                Expect.equal(after.NumValue, 0, "The updated document is not correct (NumValue)");
                Expect.isNotNull(after.Sub, "The updated document should have had a sub-document");
                Expect.equal(after.Sub!.Foo, "blue", "The updated document is not correct (Sub.Foo)");
                Expect.equal(after.Sub.Bar, "red", "The updated document is not correct (Sub.Bar)");
            }),
            TestCase("succeeds when no document is updated", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                var before = await conn.CountAll(PostgresDb.TableName);
                Expect.equal(before, 0, "There should have been no documents returned");
                
                // This not raising an exception is the test
                await conn.UpdateFull(PostgresDb.TableName, "test",
                    new JsonDocument { Id = "x", Sub = new() { Foo = "blue", Bar = "red" } });
            })
        }),
        TestList("UpdateFullFunc", new[]
        {
            TestCase("succeeds when a document is updated", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                await conn.UpdateFullFunc(PostgresDb.TableName, doc => doc.Id,
                    new JsonDocument { Id = "one", Value = "le un", NumValue = 1 });
                var after = await conn.FindById<string, JsonDocument>(PostgresDb.TableName, "one");
                Expect.isNotNull(after, "There should have been a document returned post-update");
                Expect.equal(after.Id, "one", "The updated document is not correct (ID)");
                Expect.equal(after.Value, "le un", "The updated document is not correct (Value)");
                Expect.equal(after.NumValue, 1, "The updated document is not correct (NumValue)");
                Expect.isNull(after.Sub, "The updated document should not have had a sub-document");
            }),
            TestCase("succeeds when no document is updated", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                var before = await conn.CountAll(PostgresDb.TableName);
                Expect.equal(before, 0, "There should have been no documents returned");
                
                // This not raising an exception is the test
                await conn.UpdateFullFunc(PostgresDb.TableName, doc => doc.Id,
                    new JsonDocument { Id = "one", Value = "le un", NumValue = 1 });
            })
        }),
        TestList("UpdatePartialById", new[]
        {
            TestCase("succeeds when a document is updated", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                await conn.UpdatePartialById(PostgresDb.TableName, "one", new { NumValue = 44 });
                var after = await conn.FindById<string, JsonDocument>(PostgresDb.TableName, "one");
                Expect.isNotNull(after, "There should have been a document returned post-update");
                Expect.equal(after.NumValue, 44, "The updated document is not correct");
            }),
            TestCase("succeeds when no document is updated", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                var before = await conn.CountAll(PostgresDb.TableName);
                Expect.equal(before, 0, "There should have been no documents returned");
                
                // This not raising an exception is the test
                await conn.UpdatePartialById(PostgresDb.TableName, "test", new { Foo = "green" });
            })
        }),
        TestList("UpdatePartialByField", new[]
        {
            TestCase("succeeds when a document is updated", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                await conn.UpdatePartialByField(PostgresDb.TableName, "Value", Op.EQ, "purple", new { NumValue = 77 });
                var after = await conn.CountByField(PostgresDb.TableName, "NumValue", Op.EQ, "77");
                Expect.equal(after, 2, "There should have been 2 documents returned");
            }),
            TestCase("succeeds when no document is updated", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                var before = await conn.CountAll(PostgresDb.TableName);
                Expect.equal(before, 0, "There should have been no documents returned");
                
                // This not raising an exception is the test
                await conn.UpdatePartialByField(PostgresDb.TableName, "Value", Op.EQ, "burgundy",
                    new { Foo = "green" });
            })
        }),
        TestList("UpdatePartialByContains", new[]
        {
            TestCase("succeeds when a document is updated", async () =>
            {
                await using var db   = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                await conn.UpdatePartialByContains(PostgresDb.TableName, new { Value = "purple" },
                    new { NumValue = 77 });
                var after = await conn.CountByContains(PostgresDb.TableName, new { NumValue = 77 });
                Expect.equal(after, 2, "There should have been 2 documents returned");
            }),
            TestCase("succeeds when no document is updated", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                var before = await conn.CountAll(PostgresDb.TableName);
                Expect.equal(before, 0, "There should have been no documents returned");
                
                // This not raising an exception is the test
                await conn.UpdatePartialByContains(PostgresDb.TableName, new { Value = "burgundy" },
                    new { Foo = "green" });
            })
        }),
        TestList("UpdatePartialByJsonPath", new[]
        {
            TestCase("succeeds when a document is updated", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                await conn.UpdatePartialByJsonPath(PostgresDb.TableName, "$.NumValue ? (@ > 10)",
                    new { NumValue = 1000 });
                var after = await conn.CountByJsonPath(PostgresDb.TableName, "$.NumValue ? (@ > 999)");
                Expect.equal(after, 2, "There should have been 2 documents returned");
            }),
            TestCase("succeeds when no document is updated", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                var before = await conn.CountAll(PostgresDb.TableName);
                Expect.equal(before, 0, "There should have been no documents returned");
                
                // This not raising an exception is the test
                await conn.UpdatePartialByJsonPath(PostgresDb.TableName, "$.NumValue ? (@ < 0)", new { Foo = "green" });
            })
        }),
        TestList("DeleteById", new[]
        {
            TestCase("succeeds when a document is deleted", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                await conn.DeleteById(PostgresDb.TableName, "four");
                var remaining = await conn.CountAll(PostgresDb.TableName);
                Expect.equal(remaining, 4, "There should have been 4 documents remaining");
            }),
            TestCase("succeeds when a document is not deleted", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                await conn.DeleteById(PostgresDb.TableName, "thirty");
                var remaining = await conn.CountAll(PostgresDb.TableName);
                Expect.equal(remaining, 5, "There should have been 5 documents remaining");
            })
        }),
        TestList("DeleteByField", new[]
        {
            TestCase("succeeds when documents are deleted", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                await conn.DeleteByField(PostgresDb.TableName, "Value", Op.NE, "purple");
                var remaining = await conn.CountAll(PostgresDb.TableName);
                Expect.equal(remaining, 2, "There should have been 2 documents remaining");
            }),
            TestCase("succeeds when documents are not deleted", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                await conn.DeleteByField(PostgresDb.TableName, "Value", Op.EQ, "crimson");
                var remaining = await conn.CountAll(PostgresDb.TableName);
                Expect.equal(remaining, 5, "There should have been 5 documents remaining");
            })
        }),
        TestList("DeleteByContains", new[]
        {
            TestCase("succeeds when documents are deleted", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                await conn.DeleteByContains(PostgresDb.TableName, new { Value = "purple" });
                var remaining = await conn.CountAll(PostgresDb.TableName);
                Expect.equal(remaining, 3, "There should have been 3 documents remaining");
            }),
            TestCase("succeeds when documents are not deleted", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                await conn.DeleteByContains(PostgresDb.TableName, new { Value = "crimson" });
                var remaining = await conn.CountAll(PostgresDb.TableName);
                Expect.equal(remaining, 5, "There should have been 5 documents remaining");
            })
        }),
        TestList("DeleteByJsonPath", new[]
        {
            TestCase("succeeds when documents are deleted", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                await conn.DeleteByJsonPath(PostgresDb.TableName, "$.Sub.Foo ? (@ == \"green\")");
                var remaining = await conn.CountAll(PostgresDb.TableName);
                Expect.equal(remaining, 3, "There should have been 3 documents remaining");
            }),
            TestCase("succeeds when documents are not deleted", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await using var conn = MkConn(db);
                await LoadDocs();

                await conn.DeleteByJsonPath(PostgresDb.TableName, "$.NumValue ? (@ > 100)");
                var remaining = await conn.CountAll(PostgresDb.TableName);
                Expect.equal(remaining, 5, "There should have been 5 documents remaining");
            })
        }),
    });
}
