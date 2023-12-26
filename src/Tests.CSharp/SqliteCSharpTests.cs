using Expecto.CSharp;
using Expecto;
using Microsoft.Data.Sqlite;
using Microsoft.FSharp.Core;
using BitBadger.Documents.Sqlite;

namespace BitBadger.Documents.Tests.CSharp;

using static Runner;

/// <summary>
/// C# tests for the SQLite implementation of <tt>BitBadger.Documents</tt>
/// </summary>
public static class SqliteCSharpTests
{
    /// <summary>
    /// Unit tests for the SQLite library
    /// </summary>
    public static Test Unit =
        TestList("Unit", new[]
        {
            TestList("Parameters", new[] 
            {
                TestCase("Id succeeds", () =>
                {
                    var theParam = Parameters.Id(7);
                    Expect.equal(theParam.ParameterName, "@id", "The parameter name is incorrect");
                    Expect.equal(theParam.Value, "7", "The parameter value is incorrect");
                }),
                TestCase("Json succeeds", () =>
                {
                    var theParam = Parameters.Json("@test", new { Nice = "job" });
                    Expect.equal(theParam.ParameterName, "@test", "The parameter name is incorrect");
                    Expect.equal(theParam.Value, "{\"Nice\":\"job\"}", "The parameter value is incorrect");
                }),
                TestCase("Field succeeds", () =>
                {
                    var theParam = Parameters.Field(99);
                    Expect.equal(theParam.ParameterName, "@field", "The parameter name is incorrect");
                    Expect.equal(theParam.Value, 99, "The parameter value is incorrect");
                }),
                TestCase("None succeeds", () =>
                {
                    Expect.isEmpty(Parameters.None, "The parameter list should have been empty");
                })
            })
            // Results are exhaustively executed in the context of other tests
        });

    private static readonly List<JsonDocument> TestDocuments = new()
    {
        new() { Id = "one", Value = "FIRST!", NumValue = 0 },
        new() { Id = "two", Value = "another", NumValue = 10, Sub = new() { Foo = "green", Bar = "blue" } },
        new() { Id = "three", Value = "", NumValue = 4 },
        new() { Id = "four", Value = "purple", NumValue = 17, Sub = new() { Foo = "green", Bar = "red" } },
        new() { Id = "five", Value = "purple", NumValue = 18 }
    };

    private static async Task LoadDocs()
    {
        foreach (var doc in TestDocuments) await Document.Insert(SqliteDb.TableName, doc);
    }

    [Tests]
    public static Test Integration =
        TestList("Integration", new[]
        {
            TestCase("Configuration.UseConnectionString succeeds", () =>
            {
                try
                {
                    Sqlite.Configuration.UseConnectionString("Data Source=test.db");
                    Expect.equal(Sqlite.Configuration.connectionString,
                        new FSharpOption<string>("Data Source=test.db;Foreign Keys=True"),
                        "Connection string incorrect");
                }
                finally
                {
                    Sqlite.Configuration.UseConnectionString("Data Source=:memory:");
                }
            }),
            TestList("Custom", new[]
            {
                TestList("Single", new []
                {
                    TestCase("succeeds when a row is found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();
                        
                        var doc = await Custom.Single(
                            $"SELECT data FROM {SqliteDb.TableName} WHERE data ->> 'Id' = @id",
                            new[] { Parameters.Id("one") }, Results.FromData<JsonDocument>);
                        Expect.isNotNull(doc, "There should have been a document returned");
                        Expect.equal(doc!.Id, "one", "The incorrect document was returned");
                    }),
                    TestCase("succeeds when a row is not found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();
                        
                        var doc = await Custom.Single(
                            $"SELECT data FROM {SqliteDb.TableName} WHERE data ->> 'Id' = @id",
                            new[] { Parameters.Id("eighty") }, Results.FromData<JsonDocument>);
                        Expect.isNull(doc, "There should not have been a document returned");
                    })
                }),
                TestList("List", new[]
                {
                    TestCase("succeeds when data is found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();
                        
                        var docs = await Custom.List(Query.SelectFromTable(SqliteDb.TableName), Parameters.None,
                            Results.FromData<JsonDocument>);
                        Expect.equal(docs.Count, 5, "There should have been 5 documents returned");
                    }),
                    TestCase("succeeds when data is not found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var docs = await Custom.List(
                            $"SELECT data FROM {SqliteDb.TableName} WHERE data ->> 'NumValue' > @value",
                            new[] { new SqliteParameter("@value", 100) }, Results.FromData<JsonDocument>);
                        Expect.isEmpty(docs, "There should have been no documents returned");
                    })
                }),
                TestList("NonQuery", new[]
                {
                    TestCase("succeeds when operating on data", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        await Custom.NonQuery($"DELETE FROM {SqliteDb.TableName}", Parameters.None);

                        var remaining = await Count.All(SqliteDb.TableName);
                        Expect.equal(remaining, 0L, "There should be no documents remaining in the table");
                    }),
                    TestCase("succeeds when no data matches where clause", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        await Custom.NonQuery(
                            $"DELETE FROM {SqliteDb.TableName} WHERE data ->> 'NumValue' > @value",
                            new[] { new SqliteParameter("@value", 100) });

                        var remaining = await Count.All(SqliteDb.TableName);
                        Expect.equal(remaining, 5L, "There should be 5 documents remaining in the table");
                    })
                }),
                TestCase("Scalar succeeds", async () =>
                {
                    await using var db = await SqliteDb.BuildDb();

                    var nbr = await Custom.Scalar("SELECT 5 AS test_value", Parameters.None, rdr => rdr.GetInt32(0));
                    Expect.equal(nbr, 5, "The query should have returned the number 5");
                })
            }),
            TestList("Definition", new[]
            {
                TestCase("EnsureTable succeeds", async () =>
                {
                    await using var db = await SqliteDb.BuildDb();

                    var exists = await ItExists("ensured");
                    var alsoExists = await ItExists("idx_ensured_key");
                    Expect.isFalse(exists, "The table should not exist already");
                    Expect.isFalse(alsoExists, "The key index should not exist already");

                    await Definition.EnsureTable("ensured");
                    
                    exists = await ItExists("ensured");
                    alsoExists = await ItExists("idx_ensured_key");
                    Expect.isTrue(exists, "The table should now exist");
                    Expect.isTrue(alsoExists, "The key index should now exist");
                    return;

                    async ValueTask<bool> ItExists(string name)
                    {
                        var result = await Custom.Scalar(
                            $"SELECT EXISTS (SELECT 1 FROM {SqliteDb.Catalog} WHERE name = @name) AS it",
                            new SqliteParameter[] { new("@name", name) },
                            rdr => rdr.GetInt64(0));
                        return result > 0L;
                    }
                })
            }),
            TestList("Document.Insert", new[]
            {
                TestCase("succeeds", async () =>
                {
                    await using var db = await SqliteDb.BuildDb();
                    var before = await Find.All<SubDocument>(SqliteDb.TableName);
                    Expect.isEmpty(before, "There should be no documents in the table");
                    await Document.Insert(SqliteDb.TableName,
                        new JsonDocument { Id = "turkey", Sub = new() { Foo = "gobble", Bar = "gobble" } });
                    var after = await Find.All<JsonDocument>(SqliteDb.TableName);
                    Expect.equal(after.Count, 1, "There should have been one document inserted");
                }),
                TestCase("fails for duplicate key", async () =>
                {
                    await using var db = await SqliteDb.BuildDb();
                    await Document.Insert(SqliteDb.TableName, new JsonDocument { Id = "test" });
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
            TestList("Document.Save", new[]
            {
                TestCase("succeeds when a document is inserted", async () =>
                {
                    await using var db = await SqliteDb.BuildDb();
                    var before = await Find.All<JsonDocument>(SqliteDb.TableName);
                    Expect.isEmpty(before, "There should be no documents in the table");

                    await Document.Save(SqliteDb.TableName,
                        new JsonDocument { Id = "test", Sub = new() { Foo = "a", Bar = "b" } });
                    var after = await Find.All<JsonDocument>(SqliteDb.TableName);
                    Expect.equal(after.Count, 1, "There should have been one document inserted");
                }),
                TestCase("succeeds when a document is updated", async () =>
                {
                    await using var db = await SqliteDb.BuildDb();
                    await Document.Insert(SqliteDb.TableName,
                        new JsonDocument { Id = "test", Sub = new() { Foo = "a", Bar = "b" } });

                    var before = await Find.ById<string, JsonDocument>(SqliteDb.TableName, "test");
                    Expect.isNotNull(before, "There should have been a document returned");
                    Expect.equal(before!.Id, "test", "The document is not correct");
                    Expect.isNotNull(before.Sub, "There should have been a sub-document");
                    Expect.equal(before.Sub!.Foo, "a", "The document is not correct");
                    Expect.equal(before.Sub.Bar, "b", "The document is not correct");

                    await Document.Save(SqliteDb.TableName, new JsonDocument { Id = "test" });
                    var after = await Find.ById<string, JsonDocument>(SqliteDb.TableName, "test");
                    Expect.isNotNull(after, "There should have been a document returned post-update");
                    Expect.equal(after!.Id, "test", "The updated document is not correct");
                    Expect.isNull(after.Sub, "There should not have been a sub-document in the updated document");
                })
            }),
            TestList("Count", new[]
            {
                TestCase("All succeeds", async () =>
                {
                    await using var db = await SqliteDb.BuildDb();
                    await LoadDocs();

                    var theCount = await Count.All(SqliteDb.TableName);
                    Expect.equal(theCount, 5L, "There should have been 5 matching documents");
                }),
                TestCase("ByField succeeds", async () =>
                {
                    await using var db = await SqliteDb.BuildDb();
                    await LoadDocs();

                    var theCount = await Count.ByField(SqliteDb.TableName, "Value", Op.EQ, "purple");
                    Expect.equal(theCount, 2L, "There should have been 2 matching documents");
                })
            }),
            TestList("Exists", new[]
            {
                TestList("ById", new[]
                {
                    TestCase("succeeds when a document exists", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var exists = await Exists.ById(SqliteDb.TableName, "three");
                        Expect.isTrue(exists, "There should have been an existing document");
                    }),
                    TestCase("succeeds when a document does not exist", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var exists = await Exists.ById(SqliteDb.TableName, "seven");
                        Expect.isFalse(exists, "There should not have been an existing document");
                    })
                }),
                TestList("ByField", new[]
                {
                    TestCase("succeeds when documents exist", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var exists = await Exists.ByField(SqliteDb.TableName, "NumValue", Op.GE, 10);
                        Expect.isTrue(exists, "There should have been existing documents");
                    }),
                    TestCase("succeeds when no matching documents exist", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var exists = await Exists.ByField(SqliteDb.TableName, "Nothing", Op.EQ, "none");
                        Expect.isFalse(exists, "There should not have been any existing documents");
                    })
                })
            }),
            TestList("Find", new[]
            {
                TestList("All", new[]
                {
                    TestCase("succeeds when there is data", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();

                        await Document.Insert(SqliteDb.TableName, new JsonDocument { Id = "one", Value = "two" });
                        await Document.Insert(SqliteDb.TableName, new JsonDocument { Id = "three", Value = "four" });
                        await Document.Insert(SqliteDb.TableName, new JsonDocument { Id = "five", Value = "six" });

                        var results = await Find.All<JsonDocument>(SqliteDb.TableName);
                        Expect.equal(results.Count, 3, "There should have been 3 documents returned");
                    }),
                    TestCase("succeeds when there is no data", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        var results = await Find.All<SubDocument>(SqliteDb.TableName);
                        Expect.isEmpty(results, "There should have been no documents returned");
                    })
                }),
                TestList("ById", new[]
                {
                    TestCase("succeeds when a document is found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var doc = await Find.ById<string, JsonDocument>(SqliteDb.TableName, "two");
                        Expect.isNotNull(doc, "There should have been a document returned");
                        Expect.equal(doc!.Id, "two", "The incorrect document was returned");
                    }),
                    TestCase("succeeds when a document is not found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var doc = await Find.ById<string, JsonDocument>(SqliteDb.TableName, "twenty two");
                        Expect.isNull(doc, "There should not have been a document returned");
                    })
                }),
                TestList("ByField", new[]
                {
                    TestCase("succeeds when documents are found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var docs = await Find.ByField<JsonDocument>(SqliteDb.TableName, "NumValue", Op.GT, 15);
                        Expect.equal(docs.Count, 2, "There should have been two documents returned");
                    }),
                    TestCase("succeeds when documents are not found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var docs = await Find.ByField<JsonDocument>(SqliteDb.TableName, "Value", Op.EQ, "mauve");
                        Expect.isEmpty(docs, "There should have been no documents returned");
                    })
                }),
                TestList("FirstByField", new[]
                {
                    TestCase("succeeds when a document is found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var doc = await Find.FirstByField<JsonDocument>(SqliteDb.TableName, "Value", Op.EQ, "another");
                        Expect.isNotNull(doc, "There should have been a document returned");
                        Expect.equal(doc!.Id, "two", "The incorrect document was returned");
                    }),
                    TestCase("succeeds when multiple documents are found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var doc = await Find.FirstByField<JsonDocument>(SqliteDb.TableName, "Sub.Foo", Op.EQ, "green");
                        Expect.isNotNull(doc, "There should have been a document returned");
                        Expect.contains(new[] { "two", "four" }, doc!.Id, "An incorrect document was returned");
                    }),
                    TestCase("succeeds when a document is not found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var doc = await Find.FirstByField<JsonDocument>(SqliteDb.TableName, "Value", Op.EQ, "absent");
                        Expect.isNull(doc, "There should not have been a document returned");
                    })
                })
            }),
            TestList("Update", new[]
            {
                TestList("Full", new[]
                {
                    TestCase("succeeds when a document is updated", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var testDoc = new JsonDocument { Id = "one", Sub = new() { Foo = "blue", Bar = "red" } };
                        await Update.Full(SqliteDb.TableName, "one", testDoc);
                        var after = await Find.ById<string, JsonDocument>(SqliteDb.TableName, "one");
                        Expect.isNotNull(after, "There should have been a document returned post-update");
                        Expect.equal(after!.Id, "one", "The updated document is not correct");
                        Expect.isNotNull(after.Sub, "The updated document should have had a sub-document");
                        Expect.equal(after.Sub!.Foo, "blue", "The updated sub-document is not correct");
                        Expect.equal(after.Sub.Bar, "red", "The updated sub-document is not correct");
                    }),
                    TestCase("succeeds when no document is updated", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();

                        var before = await Find.All<JsonDocument>(SqliteDb.TableName);
                        Expect.isEmpty(before, "There should have been no documents returned");
                        
                        // This not raising an exception is the test
                        await Update.Full(SqliteDb.TableName, "test",
                            new JsonDocument { Id = "x", Sub = new() { Foo = "blue", Bar = "red" } });
                    })
                }),
                TestList("FullFunc", new[]
                {
                    TestCase("succeeds when a document is updated", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        await Update.FullFunc(SqliteDb.TableName, doc => doc.Id,
                            new JsonDocument { Id = "one", Value = "le un", NumValue = 1 });
                        var after = await Find.ById<string, JsonDocument>(SqliteDb.TableName, "one");
                        Expect.isNotNull(after, "There should have been a document returned post-update");
                        Expect.equal(after!.Id, "one", "The updated document is incorrect");
                        Expect.equal(after.Value, "le un", "The updated document is incorrect");
                        Expect.equal(after.NumValue, 1, "The updated document is incorrect");
                        Expect.isNull(after.Sub, "The updated document should not have a sub-document");
                    }),
                    TestCase("succeeds when no document is updated", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();

                        var before = await Find.All<JsonDocument>(SqliteDb.TableName);
                        Expect.isEmpty(before, "There should have been no documents returned");
                        
                        // This not raising an exception is the test
                        await Update.FullFunc(SqliteDb.TableName, doc => doc.Id,
                            new JsonDocument { Id = "one", Value = "le un", NumValue = 1 });
                    })
                }),
                TestList("PartialById", new[]
                {
                    TestCase("succeeds when a document is updated", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        await Update.PartialById(SqliteDb.TableName, "one", new { NumValue = 44 });
                        var after = await Find.ById<string, JsonDocument>(SqliteDb.TableName, "one");
                        Expect.isNotNull(after, "There should have been a document returned post-update");
                        Expect.equal(after!.Id, "one", "The updated document is not correct");
                        Expect.equal(after.NumValue, 44, "The updated document is not correct");
                    }),
                    TestCase("succeeds when no document is updated", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();

                        var before = await Find.All<JsonDocument>(SqliteDb.TableName);
                        Expect.isEmpty(before, "There should have been no documents returned");
                        
                        // This not raising an exception is the test
                        await Update.PartialById(SqliteDb.TableName, "test", new { Foo = "green" });
                    })
                }),
                TestList("PartialByField", new[]
                {
                    TestCase("succeeds when a document is updated", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        await Update.PartialByField(SqliteDb.TableName, "Value", Op.EQ, "purple",
                            new { NumValue = 77 });
                        var after = await Count.ByField(SqliteDb.TableName, "NumValue", Op.EQ, 77);
                        Expect.equal(after, 2L, "There should have been 2 documents returned");
                    }),
                    TestCase("succeeds when no document is updated", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();

                        var before = await Find.All<SubDocument>(SqliteDb.TableName);
                        Expect.isEmpty(before, "There should have been no documents returned");
                        
                        // This not raising an exception is the test
                        await Update.PartialByField(SqliteDb.TableName, "Value", Op.EQ, "burgundy",
                            new { Foo = "green" });
                    })
                })
            }),
            TestList("Delete", new[]
            {
                TestList("ById", new[]
                {
                    TestCase("succeeds when a document is deleted", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        await Delete.ById(SqliteDb.TableName, "four");
                        var remaining = await Count.All(SqliteDb.TableName);
                        Expect.equal(remaining, 4L, "There should have been 4 documents remaining");
                    }),
                    TestCase("succeeds when a document is not deleted", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        await Delete.ById(SqliteDb.TableName, "thirty");
                        var remaining = await Count.All(SqliteDb.TableName);
                        Expect.equal(remaining, 5L, "There should have been 5 documents remaining");
                    })
                }),
                TestList("ByField", new[]
                {
                    TestCase("succeeds when documents are deleted", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        await Delete.ByField(SqliteDb.TableName, "Value", Op.NE, "purple");
                        var remaining = await Count.All(SqliteDb.TableName);
                        Expect.equal(remaining, 2L, "There should have been 2 documents remaining");
                    }),
                    TestCase("succeeds when documents are not deleted", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        await Delete.ByField(SqliteDb.TableName, "Value", Op.EQ, "crimson");
                        var remaining = await Count.All(SqliteDb.TableName);
                        Expect.equal(remaining, 5L, "There should have been 5 documents remaining");
                    })
                })
            }),
            TestList("Extensions", new[]
            {
                TestList("CustomSingle", new[]
                {
                    TestCase("succeeds when a row is found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await using var conn = Sqlite.Configuration.DbConn();
                        await LoadDocs();

                        var doc = await conn.CustomSingle(
                            $"SELECT data FROM {SqliteDb.TableName} WHERE data ->> 'Id' = @id",
                            new[] { Parameters.Id("one") }, Results.FromData<JsonDocument>);
                        Expect.isNotNull(doc, "There should have been a document returned");
                        Expect.equal(doc!.Id, "one", "The incorrect document was returned");
                    }),
                    TestCase("succeeds when a row is not found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await using var conn = Sqlite.Configuration.DbConn();
                        await LoadDocs();

                        var doc = await conn.CustomSingle(
                            $"SELECT data FROM {SqliteDb.TableName} WHERE data ->> 'Id' = @id",
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

                        await conn.CustomNonQuery(
                            $"DELETE FROM {SqliteDb.TableName} WHERE data ->> 'NumValue' > @value",
                            new[] { new SqliteParameter("@value", 100) });

                        var remaining = await conn.CountAll(SqliteDb.TableName);
                        Expect.equal(remaining, 5L, "There should be 5 documents remaining in the table");
                    })
                }),
                TestCase("CustomScalar succeeds", async () =>
                {
                    await using var db = await SqliteDb.BuildDb();
                    await using var conn = Sqlite.Configuration.DbConn();

                    var nbr = await conn.CustomScalar("SELECT 5 AS test_value", Parameters.None,
                        rdr => rdr.GetInt32(0));
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
                            new SqliteParameter[] { new("@name", name) },
                            rdr => rdr.GetInt64(0));
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
                            Expect.isTrue(false,
                                "An exception should have been raised for duplicate document ID insert");
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

                        var doc = await conn.FindFirstByField<JsonDocument>(SqliteDb.TableName, "Value", Op.EQ,
                            "another");
                        Expect.isNotNull(doc, "There should have been a document returned");
                        Expect.equal(doc!.Id, "two", "The incorrect document was returned");
                    }),
                    TestCase("succeeds when multiple documents are found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await using var conn = Sqlite.Configuration.DbConn();
                        await LoadDocs();

                        var doc = await conn.FindFirstByField<JsonDocument>(SqliteDb.TableName, "Sub.Foo", Op.EQ,
                            "green");
                        Expect.isNotNull(doc, "There should have been a document returned");
                        Expect.contains(new[] { "two", "four" }, doc!.Id, "An incorrect document was returned");
                    }),
                    TestCase("succeeds when a document is not found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await using var conn = Sqlite.Configuration.DbConn();
                        await LoadDocs();

                        var doc = await conn.FindFirstByField<JsonDocument>(SqliteDb.TableName, "Value", Op.EQ,
                            "absent");
                        Expect.isNull(doc, "There should not have been a document returned");
                    })
                }),
                TestList("UpdateFull", new[]
                {
                    TestCase("succeeds when a document is updated", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await using var conn = Sqlite.Configuration.DbConn();
                        await LoadDocs();

                        var testDoc = new JsonDocument { Id = "one", Sub = new() { Foo = "blue", Bar = "red" } };
                        await conn.UpdateFull(SqliteDb.TableName, "one", testDoc);
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
                        await conn.UpdateFull(SqliteDb.TableName, "test",
                            new JsonDocument { Id = "x", Sub = new() { Foo = "blue", Bar = "red" } });
                    })
                }),
                TestList("UpdateFullFunc", new[]
                {
                    TestCase("succeeds when a document is updated", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await using var conn = Sqlite.Configuration.DbConn();
                        await LoadDocs();

                        await conn.UpdateFullFunc(SqliteDb.TableName, doc => doc.Id,
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
                        await conn.UpdateFullFunc(SqliteDb.TableName, doc => doc.Id,
                            new JsonDocument { Id = "one", Value = "le un", NumValue = 1 });
                    })
                }),
                TestList("UpdatePartialById", new[]
                {
                    TestCase("succeeds when a document is updated", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await using var conn = Sqlite.Configuration.DbConn();
                        await LoadDocs();

                        await conn.UpdatePartialById(SqliteDb.TableName, "one", new { NumValue = 44 });
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
                        await conn.UpdatePartialById(SqliteDb.TableName, "test", new { Foo = "green" });
                    })
                }),
                TestList("UpdatePartialByField", new[]
                {
                    TestCase("succeeds when a document is updated", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await using var conn = Sqlite.Configuration.DbConn();
                        await LoadDocs();

                        await conn.UpdatePartialByField(SqliteDb.TableName, "Value", Op.EQ, "purple",
                            new { NumValue = 77 });
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
                        await conn.UpdatePartialByField(SqliteDb.TableName, "Value", Op.EQ, "burgundy",
                            new { Foo = "green" });
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
                })
            }),
            TestCase("Clean up database", () => Sqlite.Configuration.UseConnectionString("data source=:memory:"))
        });

    /// <summary>
    /// All tests for SQLite C# functions and methods
    /// </summary>
    public static readonly Test All = TestList("Sqlite.C#", new[] { Unit, TestSequenced(Integration) });
}
