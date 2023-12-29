using Expecto.CSharp;
using Expecto;
using Microsoft.Data.Sqlite;
using BitBadger.Documents.Sqlite;

namespace BitBadger.Documents.Tests.CSharp;

using static Runner;

/// <summary>
/// C# tests for the extensions on the <tt>SqliteConnection</tt> class
/// </summary>
public static class SqliteCSharpExtensionTests
{
    private static Task LoadDocs() => SqliteCSharpTests.LoadDocs();

    /// <summary>
    /// Integration tests for the SQLite extension methods
    /// </summary>
    [Tests]
    public static readonly Test Integration = TestList("Sqlite.C#.Extensions", new[]
    {
        TestList("CustomSingle", new[]
        {
            TestCase("succeeds when a row is found", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                var doc = await conn.CustomSingle($"SELECT data FROM {SqliteDb.TableName} WHERE data ->> 'Id' = @id",
                    new[] { Parameters.Id("one") }, Results.FromData<JsonDocument>);
                Expect.isNotNull(doc, "There should have been a document returned");
                Expect.equal(doc!.Id, "one", "The incorrect document was returned");
            }),
            TestCase("succeeds when a row is not found", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                var doc = await conn.CustomSingle($"SELECT data FROM {SqliteDb.TableName} WHERE data ->> 'Id' = @id",
                    new[] { Parameters.Id("eighty") }, Results.FromData<JsonDocument>);
                Expect.isNull(doc, "There should not have been a document returned");
            })
        }),
        TestList("CustomList", new[]
        {
            TestCase("succeeds when data is found", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                var docs = await conn.CustomList(Query.SelectFromTable(SqliteDb.TableName), Parameters.None,
                    Results.FromData<JsonDocument>);
                Expect.equal(docs.Count, 5, "There should have been 5 documents returned");
            }),
            TestCase("succeeds when data is not found", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                var docs = await conn.CustomList(
                    $"SELECT data FROM {SqliteDb.TableName} WHERE data ->> 'NumValue' > @value",
                    new[] { new SqliteParameter("@value", 100) }, Results.FromData<JsonDocument>);
                Expect.isEmpty(docs, "There should have been no documents returned");
            })
        }),
        TestList("CustomNonQuery", new[]
        {
            TestCase("succeeds when operating on data", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                await conn.CustomNonQuery($"DELETE FROM {SqliteDb.TableName}", Parameters.None);

                var remaining = await conn.CountAll(SqliteDb.TableName);
                Expect.equal(remaining, 0L, "There should be no documents remaining in the table");
            }),
            TestCase("succeeds when no data matches where clause", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                await conn.CustomNonQuery($"DELETE FROM {SqliteDb.TableName} WHERE data ->> 'NumValue' > @value",
                    new[] { new SqliteParameter("@value", 100) });

                var remaining = await conn.CountAll(SqliteDb.TableName);
                Expect.equal(remaining, 5L, "There should be 5 documents remaining in the table");
            })
        }),
        TestCase("CustomScalar succeeds", async () =>
        {
            await using var db = await SqliteDb.BuildDb();
            await using var conn = Sqlite.Configuration.DbConn();

            var nbr = await conn.CustomScalar("SELECT 5 AS test_value", Parameters.None, rdr => rdr.GetInt32(0));
            Expect.equal(nbr, 5, "The query should have returned the number 5");
        }),
        TestCase("EnsureTable succeeds", async () =>
        {
            await using var db = await SqliteDb.BuildDb();
            await using var conn = Sqlite.Configuration.DbConn();

            Func<string, ValueTask<bool>> itExists = async name =>
            {
                var result = await conn.CustomScalar(
                    $"SELECT EXISTS (SELECT 1 FROM {SqliteDb.Catalog} WHERE name = @name) AS it",
                    new SqliteParameter[] { new("@name", name) }, rdr => rdr.GetInt64(0));
                return result > 0L;
            };

            var exists = await itExists("ensured");
            var alsoExists = await itExists("idx_ensured_key");
            Expect.isFalse(exists, "The table should not exist already");
            Expect.isFalse(alsoExists, "The key index should not exist already");

            await conn.EnsureTable("ensured");

            exists = await itExists("ensured");
            alsoExists = await itExists("idx_ensured_key");
            Expect.isTrue(exists, "The table should now exist");
            Expect.isTrue(alsoExists, "The key index should now exist");
        }),
        TestList("Insert", new[]
        {
            TestCase("succeeds", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                var before = await conn.FindAll<SubDocument>(SqliteDb.TableName);
                Expect.isEmpty(before, "There should be no documents in the table");
                await conn.Insert(SqliteDb.TableName,
                    new JsonDocument { Id = "turkey", Sub = new() { Foo = "gobble", Bar = "gobble" } });
                var after = await conn.FindAll<JsonDocument>(SqliteDb.TableName);
                Expect.equal(after.Count, 1, "There should have been one document inserted");
            }),
            TestCase("fails for duplicate key", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await conn.Insert(SqliteDb.TableName, new JsonDocument { Id = "test" });
                try
                {
                    await Document.Insert(SqliteDb.TableName, new JsonDocument { Id = "test" });
                    Expect.isTrue(false, "An exception should have been raised for duplicate document ID insert");
                }
                catch (Exception)
                {
                    // This is what is supposed to happen
                }
            })
        }),
        TestList("Save", new[]
        {
            TestCase("succeeds when a document is inserted", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                var before = await conn.FindAll<JsonDocument>(SqliteDb.TableName);
                Expect.isEmpty(before, "There should be no documents in the table");

                await conn.Save(SqliteDb.TableName,
                    new JsonDocument { Id = "test", Sub = new() { Foo = "a", Bar = "b" } });
                var after = await conn.FindAll<JsonDocument>(SqliteDb.TableName);
                Expect.equal(after.Count, 1, "There should have been one document inserted");
            }),
            TestCase("succeeds when a document is updated", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await conn.Insert(SqliteDb.TableName,
                    new JsonDocument { Id = "test", Sub = new() { Foo = "a", Bar = "b" } });

                var before = await conn.FindById<string, JsonDocument>(SqliteDb.TableName, "test");
                Expect.isNotNull(before, "There should have been a document returned");
                Expect.equal(before!.Id, "test", "The document is not correct");
                Expect.isNotNull(before.Sub, "There should have been a sub-document");
                Expect.equal(before.Sub!.Foo, "a", "The document is not correct");
                Expect.equal(before.Sub.Bar, "b", "The document is not correct");

                await conn.Save(SqliteDb.TableName, new JsonDocument { Id = "test" });
                var after = await conn.FindById<string, JsonDocument>(SqliteDb.TableName, "test");
                Expect.isNotNull(after, "There should have been a document returned post-update");
                Expect.equal(after!.Id, "test", "The updated document is not correct");
                Expect.isNull(after.Sub, "There should not have been a sub-document in the updated document");
            })
        }),
        TestCase("CountAll succeeds", async () =>
        {
            await using var db = await SqliteDb.BuildDb();
            await using var conn = Sqlite.Configuration.DbConn();
            await LoadDocs();

            var theCount = await conn.CountAll(SqliteDb.TableName);
            Expect.equal(theCount, 5L, "There should have been 5 matching documents");
        }),
        TestCase("CountByField succeeds", async () =>
        {
            await using var db = await SqliteDb.BuildDb();
            await using var conn = Sqlite.Configuration.DbConn();
            await LoadDocs();

            var theCount = await conn.CountByField(SqliteDb.TableName, "Value", Op.EQ, "purple");
            Expect.equal(theCount, 2L, "There should have been 2 matching documents");
        }),
        TestList("ExistsById", new[]
        {
            TestCase("succeeds when a document exists", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                var exists = await conn.ExistsById(SqliteDb.TableName, "three");
                Expect.isTrue(exists, "There should have been an existing document");
            }),
            TestCase("succeeds when a document does not exist", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                var exists = await conn.ExistsById(SqliteDb.TableName, "seven");
                Expect.isFalse(exists, "There should not have been an existing document");
            })
        }),
        TestList("ExistsByField", new[]
        {
            TestCase("succeeds when documents exist", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                var exists = await conn.ExistsByField(SqliteDb.TableName, "NumValue", Op.GE, 10);
                Expect.isTrue(exists, "There should have been existing documents");
            }),
            TestCase("succeeds when no matching documents exist", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                var exists = await conn.ExistsByField(SqliteDb.TableName, "Nothing", Op.EQ, "none");
                Expect.isFalse(exists, "There should not have been any existing documents");
            })
        }),
        TestList("FindAll", new[]
        {
            TestCase("succeeds when there is data", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();

                await conn.Insert(SqliteDb.TableName, new JsonDocument { Id = "one", Value = "two" });
                await conn.Insert(SqliteDb.TableName, new JsonDocument { Id = "three", Value = "four" });
                await conn.Insert(SqliteDb.TableName, new JsonDocument { Id = "five", Value = "six" });

                var results = await conn.FindAll<JsonDocument>(SqliteDb.TableName);
                Expect.equal(results.Count, 3, "There should have been 3 documents returned");
            }),
            TestCase("succeeds when there is no data", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                var results = await conn.FindAll<JsonDocument>(SqliteDb.TableName);
                Expect.isEmpty(results, "There should have been no documents returned");
            })
        }),
        TestList("FindById", new[]
        {
            TestCase("succeeds when a document is found", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                var doc = await conn.FindById<string, JsonDocument>(SqliteDb.TableName, "two");
                Expect.isNotNull(doc, "There should have been a document returned");
                Expect.equal(doc!.Id, "two", "The incorrect document was returned");
            }),
            TestCase("succeeds when a document is not found", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                var doc = await conn.FindById<string, JsonDocument>(SqliteDb.TableName, "eighty-seven");
                Expect.isNull(doc, "There should not have been a document returned");
            })
        }),
        TestList("FindByField", new[]
        {
            TestCase("succeeds when documents are found", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                var docs = await conn.FindByField<JsonDocument>(SqliteDb.TableName, "NumValue", Op.GT, 15);
                Expect.equal(docs.Count, 2, "There should have been two documents returned");
            }),
            TestCase("succeeds when documents are not found", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                var docs = await conn.FindByField<JsonDocument>(SqliteDb.TableName, "Value", Op.EQ, "mauve");
                Expect.isEmpty(docs, "There should have been no documents returned");
            })
        }),
        TestList("FindFirstByField", new[]
        {
            TestCase("succeeds when a document is found", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                var doc = await conn.FindFirstByField<JsonDocument>(SqliteDb.TableName, "Value", Op.EQ, "another");
                Expect.isNotNull(doc, "There should have been a document returned");
                Expect.equal(doc!.Id, "two", "The incorrect document was returned");
            }),
            TestCase("succeeds when multiple documents are found", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                var doc = await conn.FindFirstByField<JsonDocument>(SqliteDb.TableName, "Sub.Foo", Op.EQ, "green");
                Expect.isNotNull(doc, "There should have been a document returned");
                Expect.contains(new[] { "two", "four" }, doc!.Id, "An incorrect document was returned");
            }),
            TestCase("succeeds when a document is not found", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                var doc = await conn.FindFirstByField<JsonDocument>(SqliteDb.TableName, "Value", Op.EQ, "absent");
                Expect.isNull(doc, "There should not have been a document returned");
            })
        }),
        TestList("UpdateById", new[]
        {
            TestCase("succeeds when a document is updated", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                var testDoc = new JsonDocument { Id = "one", Sub = new() { Foo = "blue", Bar = "red" } };
                await conn.UpdateById(SqliteDb.TableName, "one", testDoc);
                var after = await conn.FindById<string, JsonDocument>(SqliteDb.TableName, "one");
                Expect.isNotNull(after, "There should have been a document returned post-update");
                Expect.equal(after.Id, "one", "The updated document is not correct");
                Expect.isNotNull(after.Sub, "The updated document should have had a sub-document");
                Expect.equal(after.Sub!.Foo, "blue", "The updated sub-document is not correct");
                Expect.equal(after.Sub.Bar, "red", "The updated sub-document is not correct");
            }),
            TestCase("succeeds when no document is updated", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                var before = await conn.FindAll<JsonDocument>(SqliteDb.TableName);
                Expect.isEmpty(before, "There should have been no documents returned");

                // This not raising an exception is the test
                await conn.UpdateById(SqliteDb.TableName, "test",
                    new JsonDocument { Id = "x", Sub = new() { Foo = "blue", Bar = "red" } });
            })
        }),
        TestList("UpdateByFunc", new[]
        {
            TestCase("succeeds when a document is updated", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                await conn.UpdateByFunc(SqliteDb.TableName, doc => doc.Id,
                    new JsonDocument { Id = "one", Value = "le un", NumValue = 1 });
                var after = await conn.FindById<string, JsonDocument>(SqliteDb.TableName, "one");
                Expect.isNotNull(after, "There should have been a document returned post-update");
                Expect.equal(after.Id, "one", "The updated document is incorrect");
                Expect.equal(after.Value, "le un", "The updated document is incorrect");
                Expect.equal(after.NumValue, 1, "The updated document is incorrect");
                Expect.isNull(after.Sub, "The updated document should not have a sub-document");
            }),
            TestCase("succeeds when no document is updated", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                var before = await conn.FindAll<JsonDocument>(SqliteDb.TableName);
                Expect.isEmpty(before, "There should have been no documents returned");

                // This not raising an exception is the test
                await conn.UpdateByFunc(SqliteDb.TableName, doc => doc.Id,
                    new JsonDocument { Id = "one", Value = "le un", NumValue = 1 });
            })
        }),
        TestList("PatchById", new[]
        {
            TestCase("succeeds when a document is updated", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                await conn.PatchById(SqliteDb.TableName, "one", new { NumValue = 44 });
                var after = await conn.FindById<string, JsonDocument>(SqliteDb.TableName, "one");
                Expect.isNotNull(after, "There should have been a document returned post-update");
                Expect.equal(after.Id, "one", "The updated document is not correct");
                Expect.equal(after.NumValue, 44, "The updated document is not correct");
            }),
            TestCase("succeeds when no document is updated", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                var before = await conn.FindAll<JsonDocument>(SqliteDb.TableName);
                Expect.isEmpty(before, "There should have been no documents returned");

                // This not raising an exception is the test
                await conn.PatchById(SqliteDb.TableName, "test", new { Foo = "green" });
            })
        }),
        TestList("PatchByField", new[]
        {
            TestCase("succeeds when a document is updated", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                await conn.PatchByField(SqliteDb.TableName, "Value", Op.EQ, "purple", new { NumValue = 77 });
                var after = await conn.CountByField(SqliteDb.TableName, "NumValue", Op.EQ, 77);
                Expect.equal(after, 2L, "There should have been 2 documents returned");
            }),
            TestCase("succeeds when no document is updated", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                var before = await conn.FindAll<JsonDocument>(SqliteDb.TableName);
                Expect.isEmpty(before, "There should have been no documents returned");

                // This not raising an exception is the test
                await conn.PatchByField(SqliteDb.TableName, "Value", Op.EQ, "burgundy", new { Foo = "green" });
            })
        }),
        TestList("DeleteById", new[]
        {
            TestCase("succeeds when a document is deleted", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                await conn.DeleteById(SqliteDb.TableName, "four");
                var remaining = await conn.CountAll(SqliteDb.TableName);
                Expect.equal(remaining, 4L, "There should have been 4 documents remaining");
            }),
            TestCase("succeeds when a document is not deleted", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                await conn.DeleteById(SqliteDb.TableName, "thirty");
                var remaining = await conn.CountAll(SqliteDb.TableName);
                Expect.equal(remaining, 5L, "There should have been 5 documents remaining");
            })
        }),
        TestList("DeleteByField", new[]
        {
            TestCase("succeeds when documents are deleted", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                await conn.DeleteByField(SqliteDb.TableName, "Value", Op.NE, "purple");
                var remaining = await conn.CountAll(SqliteDb.TableName);
                Expect.equal(remaining, 2L, "There should have been 2 documents remaining");
            }),
            TestCase("succeeds when documents are not deleted", async () =>
            {
                await using var db = await SqliteDb.BuildDb();
                await using var conn = Sqlite.Configuration.DbConn();
                await LoadDocs();

                await conn.DeleteByField(SqliteDb.TableName, "Value", Op.EQ, "crimson");
                var remaining = await conn.CountAll(SqliteDb.TableName);
                Expect.equal(remaining, 5L, "There should have been 5 documents remaining");
            })
        }),
        TestCase("Clean up database", () => Sqlite.Configuration.UseConnectionString("data source=:memory:"))
    });
}
