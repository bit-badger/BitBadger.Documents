using Expecto.CSharp;
using Expecto;
using Microsoft.Data.Sqlite;
using Microsoft.FSharp.Core;
using Docs = BitBadger.Documents;

namespace BitBadger.Documents.Tests.CSharp;

using static Sqlite;

/// <summary>
/// C# tests for the SQLite implementation of <tt>BitBadger.Documents</tt>
/// </summary>
public static class SqliteCSharpTests
{
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
        foreach (var doc in TestDocuments) await Insert(SqliteDb.TableName, doc);
    }

    [Tests]
    public static Test Integration =
        Runner.TestList("Sqlite.C# Integration", new[]
        {
            Runner.TestCase("Configuration.UseConnectionString succeeds", () =>
            {
                try
                {
                    Configuration.UseConnectionString("Data Source=test.db");
                    Expect.equal(Configuration.connectionString,
                        new FSharpOption<string>("Data Source=test.db;Foreign Keys=True"),
                        "Connection string incorrect");
                }
                finally
                {
                    Configuration.UseConnectionString("Data Source=:memory:");
                }
            }),
            Runner.TestList("Custom", new[]
            {
                Runner.TestList("Single", new []
                {
                    Runner.TestCase("succeeds when a row is found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();
                        
                        var doc = await Custom.Single(
                            $"SELECT data FROM {SqliteDb.TableName} WHERE data ->> 'Id' = @id",
                            new[] { Parameters.Id("one") }, Results.FromData<JsonDocument>);
                        if (doc is null) Expect.isTrue(false, "There should have been a document returned");
                        Expect.equal(doc!.Id, "one", "The incorrect document was returned");
                    }),
                    Runner.TestCase("succeeds when a row is not found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();
                        
                        var doc = await Custom.Single(
                            $"SELECT data FROM {SqliteDb.TableName} WHERE data ->> 'Id' = @id",
                            new[] { Parameters.Id("eighty") }, Results.FromData<JsonDocument>);
                        Expect.isNull(doc, "There should not have been a document returned");
                    })
                }),
                Runner.TestList("List", new[]
                {
                    Runner.TestCase("succeeds when data is found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();
                        
                        var docs = await Custom.List(Docs.Query.SelectFromTable(SqliteDb.TableName), Parameters.None,
                            Results.FromData<JsonDocument>);
                        Expect.equal(docs.Count, 5, "There should have been 5 documents returned");
                    }),
                    Runner.TestCase("succeeds when data is not found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var docs = await Custom.List(
                            $"SELECT data FROM {SqliteDb.TableName} WHERE data ->> 'NumValue' > @value",
                            new[] { new SqliteParameter("@value", 100) }, Results.FromData<JsonDocument>);
                        Expect.isEmpty(docs, "There should have been no documents returned");
                    })
                }),
                Runner.TestList("NonQuery", new[]
                {
                    Runner.TestCase("succeeds when operating on data", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        await Custom.NonQuery($"DELETE FROM {SqliteDb.TableName}", Parameters.None);

                        var remaining = await Count.All(SqliteDb.TableName);
                        Expect.equal(remaining, 0L, "There should be no documents remaining in the table");
                    }),
                    Runner.TestCase("succeeds when no data matches where clause", async () =>
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
                Runner.TestCase("Scalar succeeds", async () =>
                {
                    await using var db = await SqliteDb.BuildDb();

                    var nbr = await Custom.Scalar("SELECT 5 AS test_value", Parameters.None, rdr => rdr.GetInt32(0));
                    Expect.equal(nbr, 5, "The query should have returned the number 5");
                })
            }),
            Runner.TestList("Definition", new[]
            {
                Runner.TestCase("EnsureTable succeeds", async () =>
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
            Runner.TestList("Insert", new[]
            {
                Runner.TestCase("succeeds", async () =>
                {
                    await using var db = await SqliteDb.BuildDb();
                    var before = await Find.All<SubDocument>(SqliteDb.TableName);
                    Expect.equal(before.Count, 0, "There should be no documents in the table");
                    await Insert(SqliteDb.TableName,
                        new JsonDocument { Id = "turkey", Sub = new() { Foo = "gobble", Bar = "gobble" } });
                    var after = await Find.All<JsonDocument>(SqliteDb.TableName);
                    Expect.equal(after.Count, 1, "There should have been one document inserted");
                }),
                Runner.TestCase("fails for duplicate key", async () =>
                {
                    await using var db = await SqliteDb.BuildDb();
                    await Insert(SqliteDb.TableName, new JsonDocument { Id = "test" });
                    try
                    {
                        await Insert(SqliteDb.TableName, new JsonDocument { Id = "test" });
                        Expect.isTrue(false, "An exception should have been raised for duplicate document ID insert");
                    }
                    catch (Exception)
                    {
                        // This is what is supposed to happen
                    }
                })
            }),
            Runner.TestList("Save", new[]
            {
                Runner.TestCase("succeeds when a document is inserted", async () =>
                {
                    await using var db = await SqliteDb.BuildDb();
                    var before = await Find.All<JsonDocument>(SqliteDb.TableName);
                    Expect.equal(before.Count, 0, "There should be no documents in the table");

                    await Save(SqliteDb.TableName,
                        new JsonDocument { Id = "test", Sub = new() { Foo = "a", Bar = "b" } });
                    var after = await Find.All<JsonDocument>(SqliteDb.TableName);
                    Expect.equal(after.Count, 1, "There should have been one document inserted");
                }),
                Runner.TestCase("succeeds when a document is updated", async () =>
                {
                    await using var db = await SqliteDb.BuildDb();
                    await Insert(SqliteDb.TableName,
                        new JsonDocument { Id = "test", Sub = new() { Foo = "a", Bar = "b" } });

                    var before = await Find.ById<string, JsonDocument>(SqliteDb.TableName, "test");
                    if (before is null) Expect.isTrue(false, "There should have been a document returned");
                    Expect.equal(before!.Id, "test", "The document is not correct");
                    Expect.isNotNull(before.Sub, "There should have been a sub-document");
                    Expect.equal(before.Sub!.Foo, "a", "The document is not correct");
                    Expect.equal(before.Sub.Bar, "b", "The document is not correct");

                    await Save(SqliteDb.TableName, new JsonDocument { Id = "test" });
                    var after = await Find.ById<string, JsonDocument>(SqliteDb.TableName, "test");
                    if (after is null) Expect.isTrue(false, "There should have been a document returned post-update");
                    Expect.equal(after!.Id, "test", "The updated document is not correct");
                    Expect.isNull(after.Sub, "There should not have been a sub-document in the updated document");
                })
            }),
            Runner.TestList("Count", new[]
            {
                Runner.TestCase("All succeeds", async () =>
                {
                    await using var db = await SqliteDb.BuildDb();
                    await LoadDocs();

                    var theCount = await Count.All(SqliteDb.TableName);
                    Expect.equal(theCount, 5L, "There should have been 5 matching documents");
                }),
                Runner.TestCase("ByField succeeds", async () =>
                {
                    await using var db = await SqliteDb.BuildDb();
                    await LoadDocs();

                    var theCount = await Count.ByField(SqliteDb.TableName, "Value", Op.EQ, "purple");
                    Expect.equal(theCount, 2L, "There should have been 2 matching documents");
                })
            }),
            Runner.TestList("Exists", new[]
            {
                Runner.TestList("ById", new[]
                {
                    Runner.TestCase("succeeds when a document exists", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var exists = await Exists.ById(SqliteDb.TableName, "three");
                        Expect.isTrue(exists, "There should have been an existing document");
                    }),
                    Runner.TestCase("succeeds when a document does not exist", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var exists = await Exists.ById(SqliteDb.TableName, "seven");
                        Expect.isFalse(exists, "There should not have been an existing document");
                    })
                }),
                Runner.TestList("ByField", new[]
                {
                    Runner.TestCase("succeeds when documents exist", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var exists = await Exists.ByField(SqliteDb.TableName, "NumValue", Op.GE, 10);
                        Expect.isTrue(exists, "There should have been existing documents");
                    }),
                    Runner.TestCase("succeeds when no matching documents exist", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var exists = await Exists.ByField(SqliteDb.TableName, "Nothing", Op.EQ, "none");
                        Expect.isFalse(exists, "There should not have been any existing documents");
                    })
                })
            }),
            Runner.TestList("Find", new[]
            {
                Runner.TestList("All", new[]
                {
                    Runner.TestCase("succeeds when there is data", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();

                        await Insert(SqliteDb.TableName, new JsonDocument { Id = "one", Value = "two" });
                        await Insert(SqliteDb.TableName, new JsonDocument { Id = "three", Value = "four" });
                        await Insert(SqliteDb.TableName, new JsonDocument { Id = "five", Value = "six" });

                        var results = await Find.All<JsonDocument>(SqliteDb.TableName);
                        Expect.equal(results.Count, 3, "There should have been 3 documents returned");
                    }),
                    Runner.TestCase("succeeds when there is no data", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        var results = await Find.All<SubDocument>(SqliteDb.TableName);
                        Expect.equal(results.Count, 0, "There should have been no documents returned");
                    })
                }),
                Runner.TestList("ById", new[]
                {
                    Runner.TestCase("succeeds when a document is found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var doc = await Find.ById<string, JsonDocument>(SqliteDb.TableName, "two");
                        if (doc is null) Expect.isTrue(false, "There should have been a document returned");
                        Expect.equal(doc!.Id, "two", "The incorrect document was returned");
                    }),
                    Runner.TestCase("succeeds when a document is not found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var doc = await Find.ById<string, JsonDocument>(SqliteDb.TableName, "twenty two");
                        Expect.isNull(doc, "There should not have been a document returned");
                    })
                }),
                Runner.TestList("ByField", new[]
                {
                    Runner.TestCase("succeeds when documents are found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var docs = await Find.ByField<JsonDocument>(SqliteDb.TableName, "NumValue", Op.GT, 15);
                        Expect.equal(docs.Count, 2, "There should have been two documents returned");
                    }),
                    Runner.TestCase("succeeds when documents are not found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var docs = await Find.ByField<JsonDocument>(SqliteDb.TableName, "Value", Op.EQ, "mauve");
                        Expect.equal(docs.Count, 0, "There should have been no documents returned");
                    })
                }),
                Runner.TestList("FirstByField", new[]
                {
                    Runner.TestCase("succeeds when a document is found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var doc = await Find.FirstByField<JsonDocument>(SqliteDb.TableName, "Value", Op.EQ, "another");
                        if (doc is null) Expect.isTrue(false, "There should have been a document returned");
                        Expect.equal(doc!.Id, "two", "The incorrect document was returned");
                    }),
                    Runner.TestCase("succeeds when multiple documents are found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var doc = await Find.FirstByField<JsonDocument>(SqliteDb.TableName, "Sub.Foo", Op.EQ, "green");
                        if (doc is null) Expect.isTrue(false, "There should have been a document returned");
                        Expect.contains(new[] { "two", "four" }, doc!.Id, "An incorrect document was returned");
                    }),
                    Runner.TestCase("succeeds when a document is not found", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var doc = await Find.FirstByField<JsonDocument>(SqliteDb.TableName, "Value", Op.EQ, "absent");
                        Expect.isNull(doc, "There should not have been a document returned");
                    })
                })
            }),
            Runner.TestList("Update", new[]
            {
                Runner.TestList("Full", new[]
                {
                    Runner.TestCase("succeeds when a document is updated", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        var testDoc = new JsonDocument { Id = "one", Sub = new() { Foo = "blue", Bar = "red" } };
                        await Update.Full(SqliteDb.TableName, "one", testDoc);
                        var after = await Find.ById<string, JsonDocument>(SqliteDb.TableName, "one");
                        if (after is null)
                            Expect.isTrue(false, "There should have been a document returned post-update");
                        Expect.equal(after!.Id, "one", "The updated document is not correct");
                        Expect.isNotNull(after.Sub, "The updated document should have had a sub-document");
                        Expect.equal(after.Sub!.Foo, "blue", "The updated sub-document is not correct");
                        Expect.equal(after.Sub.Bar, "red", "The updated sub-document is not correct");
                    }),
                    Runner.TestCase("succeeds when no document is updated", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();

                        var before = await Find.All<JsonDocument>(SqliteDb.TableName);
                        Expect.equal(before.Count, 0, "There should have been no documents returned");
                        
                        // This not raising an exception is the test
                        await Update.Full(SqliteDb.TableName, "test",
                            new JsonDocument { Id = "x", Sub = new() { Foo = "blue", Bar = "red" } });
                    })
                }),
                Runner.TestList("FullFunc", new[]
                {
                    Runner.TestCase("succeeds when a document is updated", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        await Update.FullFunc(SqliteDb.TableName, doc => doc.Id,
                            new JsonDocument { Id = "one", Value = "le un", NumValue = 1 });
                        var after = await Find.ById<string, JsonDocument>(SqliteDb.TableName, "one");
                        if (after is null)
                            Expect.isTrue(false, "There should have been a document returned post-update");
                        Expect.equal(after!.Id, "one", "The updated document is incorrect");
                        Expect.equal(after.Value, "le un", "The updated document is incorrect");
                        Expect.equal(after.NumValue, 1, "The updated document is incorrect");
                        Expect.isNull(after.Sub, "The updated document should not have a sub-document");
                    }),
                    Runner.TestCase("succeeds when no document is updated", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();

                        var before = await Find.All<JsonDocument>(SqliteDb.TableName);
                        Expect.equal(before.Count, 0, "There should have been no documents returned");
                        
                        // This not raising an exception is the test
                        await Update.FullFunc(SqliteDb.TableName, doc => doc.Id,
                            new JsonDocument { Id = "one", Value = "le un", NumValue = 1 });
                    })
                }),
                Runner.TestList("PartialById", new[]
                {
                    Runner.TestCase("succeeds when a document is updated", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        await Update.PartialById(SqliteDb.TableName, "one", new { NumValue = 44 });
                        var after = await Find.ById<string, JsonDocument>(SqliteDb.TableName, "one");
                        if (after is null)
                            Expect.isTrue(false, "There should have been a document returned post-update");
                        Expect.equal(after!.Id, "one", "The updated document is not correct");
                        Expect.equal(after.NumValue, 44, "The updated document is not correct");
                    }),
                    Runner.TestCase("succeeds when no document is updated", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();

                        var before = await Find.All<JsonDocument>(SqliteDb.TableName);
                        Expect.equal(before.Count, 0, "There should have been no documents returned");
                        
                        // This not raising an exception is the test
                        await Update.PartialById(SqliteDb.TableName, "test", new { Foo = "green" });
                    })
                }),
                Runner.TestList("PartialByField", new[]
                {
                    Runner.TestCase("succeeds when a document is updated", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        await Update.PartialByField(SqliteDb.TableName, "Value", Op.EQ, "purple",
                            new { NumValue = 77 });
                        var after = await Count.ByField(SqliteDb.TableName, "NumValue", Op.EQ, 77);
                        Expect.equal(after, 2L, "There should have been 2 documents returned");
                    }),
                    Runner.TestCase("succeeds when no document is updated", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();

                        var before = await Find.All<SubDocument>(SqliteDb.TableName);
                        Expect.equal(before.Count, 0, "There should have been no documents returned");
                        
                        // This not raising an exception is the test
                        await Update.PartialByField(SqliteDb.TableName, "Value", Op.EQ, "burgundy",
                            new { Foo = "green" });
                    })
                })
            }),
            Runner.TestList("Delete", new[]
            {
                Runner.TestList("ById", new[]
                {
                    Runner.TestCase("succeeds when a document is deleted", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        await Delete.ById(SqliteDb.TableName, "four");
                        var remaining = await Count.All(SqliteDb.TableName);
                        Expect.equal(remaining, 4L, "There should have been 4 documents remaining");
                    }),
                    Runner.TestCase("succeeds when a document is not deleted", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        await Delete.ById(SqliteDb.TableName, "thirty");
                        var remaining = await Count.All(SqliteDb.TableName);
                        Expect.equal(remaining, 5L, "There should have been 5 documents remaining");
                    })
                }),
                Runner.TestList("ByField", new[]
                {
                    Runner.TestCase("succeeds when documents are deleted", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        await Delete.ByField(SqliteDb.TableName, "Value", Op.NE, "purple");
                        var remaining = await Count.All(SqliteDb.TableName);
                        Expect.equal(remaining, 2L, "There should have been 2 documents remaining");
                    }),
                    Runner.TestCase("succeeds when documents are not deleted", async () =>
                    {
                        await using var db = await SqliteDb.BuildDb();
                        await LoadDocs();

                        await Delete.ByField(SqliteDb.TableName, "Value", Op.EQ, "crimson");
                        var remaining = await Count.All(SqliteDb.TableName);
                        Expect.equal(remaining, 5L, "There should have been 5 documents remaining");
                    })
                })
            }),
    //     Runner.TestList("Extensions" [
    //         Runner.TestCase("EnsureTable succeeds" {
    //             use! db   = Db.buildDb ()
    //             use  conn = Configuration.DbConn()
    //             let itExists (name: string) = task {
    //                 let! result =
    //                     conn.CustomScalar(
    //                         $"SELECT EXISTS (SELECT 1 FROM {Db.catalog} WHERE name = @name) AS it",
    //                         [ SqliteParameter("@name", name) ],
    //                         System.Func<SqliteDataReader, int64> _.GetInt64(0))
    //                 return result > 0L
    //             }
    //             
    //             let! exists     = itExists "ensured"
    //             let! alsoExists = itExists "idx_ensured_key"
    //             Expect.isFalse exists     "The table should not exist already"
    //             Expect.isFalse alsoExists "The key index should not exist already"
    //     
    //             await Definition.EnsureTable "ensured"
    //             let! exists'     = itExists "ensured"
    //             let! alsoExists' = itExists "idx_ensured_key"
    //             Expect.isTrue exists'    "The table should now exist"
    //             Expect.isTrue alsoExists' "The key index should now exist"
    //         }
    //         Runner.TestList("Insert" [
    //             Runner.TestCase("succeeds" {
    //                 use! db     = Db.buildDb ()
    //                 use  conn   = Configuration.DbConn()
    //                 let! before = conn.FindAll<SubDocument> SqliteDb.TableName 
    //                 Expect.hasCountOf before 0u isTrue "There should be no documents in the table"
    //                 await conn.Insert(
    //                         SqliteDb.TableName ,
    //                         JsonDocument(Id = "turkey", Sub = Some (SubDocument(Foo = "gobble", Bar = "gobble"))))
    //                 let! after = conn.FindAll<JsonDocument> SqliteDb.TableName 
    //                 Expect.hasCountOf after 1u isTrue "There should have been one document inserted"
    //             }
    //             Runner.TestCase("fails for duplicate key" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await conn.Insert(SqliteDb.TableName , JsonDocument(Id = "test"))
    //                 Expect.throws
    //                     (fun () ->
    //                         conn.Insert(SqliteDb.TableName , JsonDocument(Id = "test"))
    //                         |> Async.AwaitTask
    //                         |> Async.RunSynchronously)
    //                     "An exception should have been raised for duplicate document ID insert"
    //             }
    //         ]
    //         Runner.TestList("Save" [
    //             Runner.TestCase("succeeds when a document is inserted" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 let! before = conn.FindAll<JsonDocument> SqliteDb.TableName 
    //                 Expect.hasCountOf before 0u isTrue "There should be no documents in the table"
    //         
    //                 await conn.Save(
    //                         SqliteDb.TableName ,
    //                         JsonDocument(Id = "test", Sub = Some (SubDocument(Foo = "a", Bar = "b"))))
    //                 let! after = conn.FindAll<JsonDocument> SqliteDb.TableName 
    //                 Expect.hasCountOf after 1u isTrue "There should have been one document inserted"
    //             }
    //             Runner.TestCase("succeeds when a document is updated" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await conn.Insert(
    //                         SqliteDb.TableName ,
    //                         JsonDocument(Id = "test", Sub = Some (SubDocument(Foo = "a", Bar = "b"))))
    //         
    //                 let! before = conn.FindById<string, JsonDocument>(SqliteDb.TableName , "test")
    //                 if isNull before then Expect.isTrue false "There should have been a document returned"
    //                 let before = before :> JsonDocument
    //                 Expect.equal before.Id "test" "The document is not correct"
    //                 Expect.isSome before.Sub "There should have been a sub-document"
    //                 Expect.equal before.Sub.Value.Foo "a" "The document is not correct"
    //                 Expect.equal before.Sub.Value.Bar "b" "The document is not correct"
    //         
    //                 await Save(SqliteDb.TableName , JsonDocument(Id = "test"))
    //                 let! after = conn.FindById<string, JsonDocument>(SqliteDb.TableName , "test")
    //                 if isNull after then Expect.isTrue false "There should have been a document returned post-update"
    //                 let after = after :> JsonDocument
    //                 Expect.equal after.Id "test" "The updated document is not correct"
    //                 Expect.isNone after.Sub "There should not have been a sub-document in the updated document"
    //             }
    //         ]
    //         Runner.TestCase("CountAll succeeds" {
    //             use! db   = Db.buildDb ()
    //             use  conn = Configuration.DbConn()
    //             await LoadDocs();
    //     
    //             let! theCount = conn.CountAll SqliteDb.TableName 
    //             Expect.equal theCount 5L "There should have been 5 matching documents"
    //         }
    //         Runner.TestCase("CountByField succeeds" {
    //             use! db   = Db.buildDb ()
    //             use  conn = Configuration.DbConn()
    //             await LoadDocs();
    //     
    //             let! theCount = conn.CountByField(SqliteDb.TableName , "Value", Op.EQ, "purple")
    //             Expect.equal theCount 2L "There should have been 2 matching documents"
    //         }
    //         Runner.TestList("ExistsById" [
    //             Runner.TestCase("succeeds when a document exists" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //     
    //                 let! exists = conn.ExistsById(SqliteDb.TableName , "three")
    //                 Expect.isTrue exists "There should have been an existing document"
    //             }
    //             Runner.TestCase("succeeds when a document does not exist" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //     
    //                 let! exists = conn.ExistsById(SqliteDb.TableName , "seven")
    //                 Expect.isFalse exists "There should not have been an existing document"
    //             }
    //         ]
    //         Runner.TestList("ExistsByField" [
    //             Runner.TestCase("succeeds when documents exist" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //     
    //                 let! exists = conn.ExistsByField(SqliteDb.TableName , "NumValue", Op.GE, 10)
    //                 Expect.isTrue exists "There should have been existing documents"
    //             }
    //             Runner.TestCase("succeeds when no matching documents exist" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //     
    //                 let! exists = conn.ExistsByField(SqliteDb.TableName , "Nothing", Op.EQ, "none")
    //                 Expect.isFalse exists "There should not have been any existing documents"
    //             }
    //         ]
    //         Runner.TestList("FindAll" [
    //             Runner.TestCase("succeeds when there is data" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //     
    //                 await conn.Insert(SqliteDb.TableName , JsonDocument(Id = "one", Value = "two"))
    //                 await conn.Insert(SqliteDb.TableName , JsonDocument(Id = "three", Value = "four"))
    //                 await conn.Insert(SqliteDb.TableName , JsonDocument(Id = "five", Value = "six"))
    //     
    //                 let! results = conn.FindAll<SubDocument> SqliteDb.TableName 
    //                 Expect.hasCountOf results 3u isTrue "There should have been 3 documents returned"
    //             }
    //             Runner.TestCase("succeeds when there is no data" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 let! results = conn.FindAll<SubDocument> SqliteDb.TableName 
    //                 Expect.hasCountOf results 0u isTrue "There should have been no documents returned"
    //             }
    //         ]
    //         Runner.TestList("FindById" [
    //             Runner.TestCase("succeeds when a document is found" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //     
    //                 let! doc = conn.FindById<string, JsonDocument>(SqliteDb.TableName , "two")
    //                 if isNull doc then Expect.isTrue false "There should have been a document returned"
    //                 Expect.equal (doc :> JsonDocument).Id "two" "The incorrect document was returned"
    //             }
    //             Runner.TestCase("succeeds when a document is not found" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //     
    //                 let! doc = conn.FindById<string, JsonDocument>(SqliteDb.TableName , "three hundred eighty-seven")
    //                 Expect.isNull doc "There should not have been a document returned"
    //             }
    //         ]
    //         Runner.TestList("FindByField" [
    //             Runner.TestCase("succeeds when documents are found" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //     
    //                 let! docs = conn.FindByField<JsonDocument>(SqliteDb.TableName , "NumValue", Op.GT, 15)
    //                 Expect.hasCountOf docs 2u isTrue "There should have been two documents returned"
    //             }
    //             Runner.TestCase("succeeds when documents are not found" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //     
    //                 let! docs = conn.FindByField<JsonDocument>(SqliteDb.TableName , "Value", Op.EQ, "mauve")
    //                 Expect.hasCountOf docs 0u isTrue "There should have been no documents returned"
    //             }
    //         ]
    //         Runner.TestList("FindFirstByField" [
    //             Runner.TestCase("succeeds when a document is found" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //     
    //                 let! doc = conn.FindFirstByField<JsonDocument>(SqliteDb.TableName , "Value", Op.EQ, "another")
    //                 if isNull doc then Expect.isTrue false "There should have been a document returned"
    //                 Expect.equal (doc :> JsonDocument).Id "two" "The incorrect document was returned"
    //             }
    //             Runner.TestCase("succeeds when multiple documents are found" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //     
    //                 let! doc = conn.FindFirstByField<JsonDocument>(SqliteDb.TableName , "Sub.Foo", Op.EQ, "green")
    //                 if isNull doc then Expect.isTrue false "There should have been a document returned"
    //                 Expect.contains [ "two"; "four" ] (doc :> JsonDocument).Id "An incorrect document was returned"
    //             }
    //             Runner.TestCase("succeeds when a document is not found" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //     
    //                 let! doc = conn.FindFirstByField<JsonDocument>(SqliteDb.TableName , "Value", Op.EQ, "absent")
    //                 Expect.isNull doc "There should not have been a document returned"
    //             }
    //         ]
    //         Runner.TestList("UpdateFull" [
    //             Runner.TestCase("succeeds when a document is updated" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //                 
    //                 let testDoc = JsonDocument(Id = "one", Sub = Some (SubDocument(Foo = "blue", Bar = "red")))
    //                 await conn.UpdateFull(SqliteDb.TableName , "one", testDoc)
    //                 let! after = conn.FindById<string, JsonDocument>(SqliteDb.TableName , "one")
    //                 if isNull after then Expect.isTrue false "There should have been a document returned post-update"
    //                 let after = after :> JsonDocument
    //                 Expect.equal after.Id "one" "The updated document is not correct"
    //                 Expect.isSome after.Sub "The updated document should have had a sub-document"
    //                 Expect.equal after.Sub.Value.Foo "blue" "The updated sub-document is not correct"
    //                 Expect.equal after.Sub.Value.Bar "red" "The updated sub-document is not correct"
    //             }
    //             Runner.TestCase("succeeds when no document is updated" {
    //                 use! db     = Db.buildDb ()
    //                 use  conn   = Configuration.DbConn()
    //                 let! before = conn.FindAll<JsonDocument> SqliteDb.TableName 
    //                 Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
    //                 
    //                 // This not raising an exception is the test
    //                 await conn.UpdateFull(
    //                         SqliteDb.TableName ,
    //                         "test",
    //                         JsonDocument(Id = "x", Sub = Some (SubDocument(Foo = "blue", Bar = "red"))))
    //             }
    //         ]
    //         Runner.TestList("UpdateFullFunc" [
    //             Runner.TestCase("succeeds when a document is updated" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //     
    //                 await conn.UpdateFullFunc(
    //                     SqliteDb.TableName ,
    //                     System.Func<JsonDocument, string> _.Id,
    //                     JsonDocument(Id = "one", Value = "le un", NumValue = 1, Sub = None))
    //                 let! after = conn.FindById<string, JsonDocument>(SqliteDb.TableName , "one")
    //                 if isNull after then Expect.isTrue false "There should have been a document returned post-update"
    //                 let after = after :> JsonDocument
    //                 Expect.equal after.Id "one" "The updated document is incorrect"
    //                 Expect.equal after.Value "le un" "The updated document is incorrect"
    //                 Expect.equal after.NumValue 1 "The updated document is incorrect"
    //                 Expect.isNone after.Sub "The updated document should not have a sub-document"
    //             }
    //             Runner.TestCase("succeeds when no document is updated" {
    //                 use! db     = Db.buildDb ()
    //                 use  conn   = Configuration.DbConn()
    //                 let! before = conn.FindAll<JsonDocument> SqliteDb.TableName 
    //                 Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
    //                 
    //                 // This not raising an exception is the test
    //                 await conn.UpdateFullFunc(
    //                     SqliteDb.TableName ,
    //                     System.Func<JsonDocument, string> _.Id,
    //                     JsonDocument(Id = "one", Value = "le un", NumValue = 1, Sub = None))
    //             }
    //         ]
    //         Runner.TestList("UpdatePartialById" [
    //             Runner.TestCase("succeeds when a document is updated" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //                 
    //                 await conn.UpdatePartialById(SqliteDb.TableName , "one", {| NumValue = 44 |})
    //                 let! after = conn.FindById<string, JsonDocument>(SqliteDb.TableName , "one")
    //                 if isNull after then Expect.isTrue false "There should have been a document returned post-update"
    //                 let after = after :> JsonDocument
    //                 Expect.equal after.Id "one" "The updated document is not correct"
    //                 Expect.equal after.NumValue 44 "The updated document is not correct"
    //             }
    //             Runner.TestCase("succeeds when no document is updated" {
    //                 use! db     = Db.buildDb ()
    //                 use  conn   = Configuration.DbConn()
    //                 let! before = conn.FindAll<SubDocument> SqliteDb.TableName 
    //                 Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
    //                 
    //                 // This not raising an exception is the test
    //                 await conn.UpdatePartialById(SqliteDb.TableName , "test", {| Foo = "green" |})
    //             }
    //         ]
    //         Runner.TestList("UpdatePartialByField" [
    //             Runner.TestCase("succeeds when a document is updated" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //                 
    //                 await conn.UpdatePartialByField(SqliteDb.TableName , "Value", Op.EQ, "purple", {| NumValue = 77 |})
    //                 let! after = conn.CountByField(SqliteDb.TableName , "NumValue", Op.EQ, 77)
    //                 Expect.equal after 2L "There should have been 2 documents returned"
    //             }
    //             Runner.TestCase("succeeds when no document is updated" {
    //                 use! db     = Db.buildDb ()
    //                 use  conn   = Configuration.DbConn()
    //                 let! before = conn.FindAll<SubDocument> SqliteDb.TableName 
    //                 Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
    //                 
    //                 // This not raising an exception is the test
    //                 await conn.UpdatePartialByField(SqliteDb.TableName , "Value", Op.EQ, "burgundy", {| Foo = "green" |})
    //             }
    //         ]
    //         Runner.TestList("DeleteById" [
    //             Runner.TestCase("succeeds when a document is deleted" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //     
    //                 await conn.DeleteById(SqliteDb.TableName , "four")
    //                 let! remaining = conn.CountAll SqliteDb.TableName 
    //                 Expect.equal remaining 4L "There should have been 4 documents remaining"
    //             }
    //             Runner.TestCase("succeeds when a document is not deleted" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //     
    //                 await conn.DeleteById(SqliteDb.TableName , "thirty")
    //                 let! remaining = conn.CountAll SqliteDb.TableName 
    //                 Expect.equal remaining 5L "There should have been 5 documents remaining"
    //             }
    //         ]
    //         Runner.TestList("DeleteByField" [
    //             Runner.TestCase("succeeds when documents are deleted" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //     
    //                 await conn.DeleteByField(SqliteDb.TableName , "Value", Op.NE, "purple")
    //                 let! remaining = conn.CountAll SqliteDb.TableName 
    //                 Expect.equal remaining 2L "There should have been 2 documents remaining"
    //             }
    //             Runner.TestCase("succeeds when documents are not deleted" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //     
    //                 await conn.DeleteByField(SqliteDb.TableName , "Value", Op.EQ, "crimson")
    //                 let! remaining = Count.All SqliteDb.TableName 
    //                 Expect.equal remaining 5L "There should have been 5 documents remaining"
    //             }
    //         ]
    //         Runner.TestList("CustomSingle" [
    //             Runner.TestCase("succeeds when a row is found" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //     
    //                 let! doc =
    //                     conn.CustomSingle(
    //                         $"SELECT data FROM {SqliteDb.TableName } WHERE data ->> 'Id' = @id",
    //                         [ SqliteParameter("@id", "one") ],
    //                         FromData<JsonDocument>)
    //                 if isNull doc then Expect.isTrue false "There should have been a document returned"
    //                 Expect.equal (doc :> JsonDocument).Id "one" "The incorrect document was returned"
    //             }
    //             Runner.TestCase("succeeds when a row is not found" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //     
    //                 let! doc =
    //                     conn.CustomSingle(
    //                         $"SELECT data FROM {SqliteDb.TableName } WHERE data ->> 'Id' = @id",
    //                         [ SqliteParameter("@id", "eighty") ],
    //                         FromData<JsonDocument>)
    //                 Expect.isNull doc "There should not have been a document returned"
    //             }
    //         ]
    //         Runner.TestList("CustomList" [
    //             Runner.TestCase("succeeds when data is found" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //     
    //                 let! docs = conn.CustomList(Query.SelectFromTable SqliteDb.TableName , [], FromData<JsonDocument>)
    //                 Expect.hasCountOf docs 5u isTrue "There should have been 5 documents returned"
    //             }
    //             Runner.TestCase("succeeds when data is not found" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //     
    //                 let! docs =
    //                     conn.CustomList(
    //                         $"SELECT data FROM {SqliteDb.TableName } WHERE data ->> 'NumValue' > @value",
    //                         [ SqliteParameter("@value", 100) ],
    //                         FromData<JsonDocument>)
    //                 Expect.isEmpty docs "There should have been no documents returned"
    //             }
    //         ]
    //         Runner.TestList("CustomNonQuery" [
    //             Runner.TestCase("succeeds when operating on data" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //
    //                 await conn.CustomNonQuery($"DELETE FROM {SqliteDb.TableName }", [])
    //
    //                 let! remaining = conn.CountAll SqliteDb.TableName 
    //                 Expect.equal remaining 0L "There should be no documents remaining in the table"
    //             }
    //             Runner.TestCase("succeeds when no data matches where clause" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 await LoadDocs();
    //
    //                 await conn.CustomNonQuery(
    //                         $"DELETE FROM {SqliteDb.TableName } WHERE data ->> 'NumValue' > @value",
    //                         [ SqliteParameter("@value", 100) ])
    //
    //                 let! remaining = conn.CountAll SqliteDb.TableName 
    //                 Expect.equal remaining 5L "There should be 5 documents remaining in the table"
    //             }
    //         ]
    //         Runner.TestCase("CustomScalar succeeds" {
    //             use! db   = Db.buildDb ()
    //             use  conn = Configuration.DbConn()
    //     
    //             let! nbr =
    //                 conn.CustomScalar("SELECT 5 AS test_value", [], System.Func<SqliteDataReader, int> _.GetInt32(0))
    //             Expect.equal nbr 5 "The query should have returned the number 5"
    //         }
    //     ]
    //     test "clean up database" {
    //         Configuration.UseConnectionString "data source=:memory:"
    //     }
    //         }
    // ]
    // |> testSequenced)
        });
}
