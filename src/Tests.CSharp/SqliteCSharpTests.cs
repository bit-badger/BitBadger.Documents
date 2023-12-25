using Microsoft.Data.Sqlite;
using Microsoft.FSharp.Core;

namespace BitBadger.Documents.Tests.CSharp;

using Expecto.CSharp;
using Expecto;
using static Sqlite;

/// <summary>
/// C# tests for the SQLite implementation of <tt>BitBadger.Documents</tt>
/// </summary>
public static class SqliteCSharpTests
{
    private static readonly List<JsonDocument> Documents = new()
    {
        new() { Id = "one", Value = "FIRST!", NumValue = 0 },
        new() { Id = "two", Value = "another", NumValue = 10, Sub = new() { Foo = "green", Bar = "blue" } },
        new() { Id = "three", Value = "", NumValue = 4 },
        new() { Id = "four", Value = "purple", NumValue = 17, Sub = new() { Foo = "green", Bar = "red" } },
        new() { Id = "five", Value = "purple", NumValue = 18 }
    };

    private static async Task LoadDocs()
    {
        foreach (var doc in Documents) await Insert(SqliteDb.TableName, doc);
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
    //     testList "Find" [
    //         testList "All" [
    //             testTask "succeeds when there is data" {
    //                 use! db = Db.buildDb ()
    //     
    //                 do! Insert(Db.tableName, JsonDocument(Id = "one", Value = "two"))
    //                 do! Insert(Db.tableName, JsonDocument(Id = "three", Value = "four"))
    //                 do! Insert(Db.tableName, JsonDocument(Id = "five", Value = "six"))
    //     
    //                 let! results = Find.All<SubDocument> Db.tableName
    //                 Expect.hasCountOf results 3u isTrue "There should have been 3 documents returned"
    //             }
    //             testTask "succeeds when there is no data" {
    //                 use! db = Db.buildDb ()
    //                 let! results = Find.All<SubDocument> Db.tableName
    //                 Expect.hasCountOf results 0u isTrue "There should have been no documents returned"
    //             }
    //         ]
    //         testList "ById" [
    //             testTask "succeeds when a document is found" {
    //                 use! db = Db.buildDb ()
    //                 do! loadDocs ()
    //     
    //                 let! doc = Find.ById<string, JsonDocument>(Db.tableName, "two")
    //                 if isNull doc then Expect.isTrue false "There should have been a document returned"
    //                 Expect.equal (doc :> JsonDocument).Id "two" "The incorrect document was returned"
    //             }
    //             testTask "succeeds when a document is not found" {
    //                 use! db = Db.buildDb ()
    //                 do! loadDocs ()
    //     
    //                 let! doc = Find.ById<string, JsonDocument>(Db.tableName, "three hundred eighty-seven")
    //                 Expect.isNull doc "There should not have been a document returned"
    //             }
    //         ]
    //         testList "ByField" [
    //             testTask "succeeds when documents are found" {
    //                 use! db = Db.buildDb ()
    //                 do! loadDocs ()
    //     
    //                 let! docs = Find.ByField<JsonDocument>(Db.tableName, "NumValue", Op.GT, 15)
    //                 Expect.hasCountOf docs 2u isTrue "There should have been two documents returned"
    //             }
    //             testTask "succeeds when documents are not found" {
    //                 use! db = Db.buildDb ()
    //                 do! loadDocs ()
    //     
    //                 let! docs = Find.ByField<JsonDocument>(Db.tableName, "Value", Op.EQ, "mauve")
    //                 Expect.hasCountOf docs 0u isTrue "There should have been no documents returned"
    //             }
    //         ]
    //         testList "FirstByField" [
    //             testTask "succeeds when a document is found" {
    //                 use! db = Db.buildDb ()
    //                 do! loadDocs ()
    //     
    //                 let! doc = Find.FirstByField<JsonDocument>(Db.tableName, "Value", Op.EQ, "another")
    //                 if isNull doc then Expect.isTrue false "There should have been a document returned"
    //                 Expect.equal (doc :> JsonDocument).Id "two" "The incorrect document was returned"
    //             }
    //             testTask "succeeds when multiple documents are found" {
    //                 use! db = Db.buildDb ()
    //                 do! loadDocs ()
    //     
    //                 let! doc = Find.FirstByField<JsonDocument>(Db.tableName, "Sub.Foo", Op.EQ, "green")
    //                 if isNull doc then Expect.isTrue false "There should have been a document returned"
    //                 Expect.contains [ "two"; "four" ] (doc :> JsonDocument).Id "An incorrect document was returned"
    //             }
    //             testTask "succeeds when a document is not found" {
    //                 use! db = Db.buildDb ()
    //                 do! loadDocs ()
    //     
    //                 let! doc = Find.FirstByField<JsonDocument>(Db.tableName, "Value", Op.EQ, "absent")
    //                 Expect.isNull doc "There should not have been a document returned"
    //             }
    //         ]
    //     ]
    //     testList "Update" [
    //         testList "Full" [
    //             testTask "succeeds when a document is updated" {
    //                 use! db = Db.buildDb ()
    //                 do! loadDocs ()
    //     
    //                 let testDoc = JsonDocument(Id = "one", Sub = Some (SubDocument(Foo = "blue", Bar = "red")))
    //                 do! Update.Full(Db.tableName, "one", testDoc)
    //                 let! after = Find.ById<string, JsonDocument>(Db.tableName, "one")
    //                 if isNull after then Expect.isTrue false "There should have been a document returned post-update"
    //                 let after = after :> JsonDocument
    //                 Expect.equal after.Id "one" "The updated document is not correct"
    //                 Expect.isSome after.Sub "The updated document should have had a sub-document"
    //                 Expect.equal after.Sub.Value.Foo "blue" "The updated sub-document is not correct"
    //                 Expect.equal after.Sub.Value.Bar "red" "The updated sub-document is not correct"
    //             }
    //             testTask "succeeds when no document is updated" {
    //                 use! db = Db.buildDb ()
    //     
    //                 let! before = Find.All<JsonDocument> Db.tableName
    //                 Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
    //                 
    //                 // This not raising an exception is the test
    //                 do! Update.Full(
    //                         Db.tableName,
    //                         "test",
    //                         JsonDocument(Id = "x", Sub = Some (SubDocument(Foo = "blue", Bar = "red"))))
    //             }
    //         ]
    //         testList "FullFunc" [
    //             testTask "succeeds when a document is updated" {
    //                 use! db = Db.buildDb ()
    //                 do! loadDocs ()
    //     
    //                 do! Update.FullFunc(
    //                     Db.tableName,
    //                     System.Func<JsonDocument, string> _.Id,
    //                     JsonDocument(Id = "one", Value = "le un", NumValue = 1, Sub = None))
    //                 let! after = Find.ById<string, JsonDocument>(Db.tableName, "one")
    //                 if isNull after then Expect.isTrue false "There should have been a document returned post-update"
    //                 let after = after :> JsonDocument
    //                 Expect.equal after.Id "one" "The updated document is incorrect"
    //                 Expect.equal after.Value "le un" "The updated document is incorrect"
    //                 Expect.equal after.NumValue 1 "The updated document is incorrect"
    //                 Expect.isNone after.Sub "The updated document should not have a sub-document"
    //             }
    //             testTask "succeeds when no document is updated" {
    //                 use! db = Db.buildDb ()
    //     
    //                 let! before = Find.All<JsonDocument> Db.tableName
    //                 Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
    //                 
    //                 // This not raising an exception is the test
    //                 do! Update.FullFunc(
    //                     Db.tableName,
    //                     System.Func<JsonDocument, string> _.Id,
    //                     JsonDocument(Id = "one", Value = "le un", NumValue = 1, Sub = None))
    //             }
    //         ]
    //         testList "PartialById" [
    //             testTask "succeeds when a document is updated" {
    //                 use! db = Db.buildDb ()
    //                 do! loadDocs ()
    //                 
    //                 do! Update.PartialById(Db.tableName, "one", {| NumValue = 44 |})
    //                 let! after = Find.ById<string, JsonDocument>(Db.tableName, "one")
    //                 if isNull after then Expect.isTrue false "There should have been a document returned post-update"
    //                 let after = after :> JsonDocument
    //                 Expect.equal after.Id "one" "The updated document is not correct"
    //                 Expect.equal after.NumValue 44 "The updated document is not correct"
    //             }
    //             testTask "succeeds when no document is updated" {
    //                 use! db = Db.buildDb ()
    //     
    //                 let! before = Find.All<SubDocument> Db.tableName
    //                 Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
    //                 
    //                 // This not raising an exception is the test
    //                 do! Update.PartialById(Db.tableName, "test", {| Foo = "green" |})
    //             }
    //         ]
    //         testList "PartialByField" [
    //             testTask "succeeds when a document is updated" {
    //                 use! db = Db.buildDb ()
    //                 do! loadDocs ()
    //                 
    //                 do! Update.PartialByField(Db.tableName, "Value", Op.EQ, "purple", {| NumValue = 77 |})
    //                 let! after = Count.ByField(Db.tableName, "NumValue", Op.EQ, 77)
    //                 Expect.equal after 2L "There should have been 2 documents returned"
    //             }
    //             testTask "succeeds when no document is updated" {
    //                 use! db = Db.buildDb ()
    //     
    //                 let! before = Find.All<SubDocument> Db.tableName
    //                 Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
    //                 
    //                 // This not raising an exception is the test
    //                 do! Update.PartialByField(Db.tableName, "Value", Op.EQ, "burgundy", {| Foo = "green" |})
    //             }
    //         ]
    //     ]
    //     testList "Delete" [
    //         testList "ById" [
    //             testTask "succeeds when a document is deleted" {
    //                 use! db = Db.buildDb ()
    //                 do! loadDocs ()
    //     
    //                 do! Delete.ById(Db.tableName, "four")
    //                 let! remaining = Count.All Db.tableName
    //                 Expect.equal remaining 4L "There should have been 4 documents remaining"
    //             }
    //             testTask "succeeds when a document is not deleted" {
    //                 use! db = Db.buildDb ()
    //                 do! loadDocs ()
    //     
    //                 do! Delete.ById(Db.tableName, "thirty")
    //                 let! remaining = Count.All Db.tableName
    //                 Expect.equal remaining 5L "There should have been 5 documents remaining"
    //             }
    //         ]
    //         testList "ByField" [
    //             testTask "succeeds when documents are deleted" {
    //                 use! db = Db.buildDb ()
    //                 do! loadDocs ()
    //     
    //                 do! Delete.ByField(Db.tableName, "Value", Op.NE, "purple")
    //                 let! remaining = Count.All Db.tableName
    //                 Expect.equal remaining 2L "There should have been 2 documents remaining"
    //             }
    //             testTask "succeeds when documents are not deleted" {
    //                 use! db = Db.buildDb ()
    //                 do! loadDocs ()
    //     
    //                 do! Delete.ByField(Db.tableName, "Value", Op.EQ, "crimson")
    //                 let! remaining = Count.All Db.tableName
    //                 Expect.equal remaining 5L "There should have been 5 documents remaining"
    //             }
    //         ]
    //     ]
    //     testList "Custom" [
    //         testList "Single" [
    //             testTask "succeeds when a row is found" {
    //                 use! db = Db.buildDb ()
    //                 do! loadDocs ()
    //     
    //                 let! doc =
    //                     Custom.Single(
    //                         $"SELECT data FROM {Db.tableName} WHERE data ->> 'Id' = @id",
    //                         [ SqliteParameter("@id", "one") ],
    //                         FromData<JsonDocument>)
    //                 if isNull doc then Expect.isTrue false "There should have been a document returned"
    //                 Expect.equal (doc :> JsonDocument).Id "one" "The incorrect document was returned"
    //             }
    //             testTask "succeeds when a row is not found" {
    //                 use! db = Db.buildDb ()
    //                 do! loadDocs ()
    //     
    //                 let! doc =
    //                     Custom.Single(
    //                         $"SELECT data FROM {Db.tableName} WHERE data ->> 'Id' = @id",
    //                         [ SqliteParameter("@id", "eighty") ],
    //                         FromData<JsonDocument>)
    //                 Expect.isNull doc "There should not have been a document returned"
    //             }
    //         ]
    //         testList "List" [
    //             testTask "succeeds when data is found" {
    //                 use! db = Db.buildDb ()
    //                 do! loadDocs ()
    //     
    //                 let! docs = Custom.List(Query.SelectFromTable Db.tableName, [], FromData<JsonDocument>)
    //                 Expect.hasCountOf docs 5u isTrue "There should have been 5 documents returned"
    //             }
    //             testTask "succeeds when data is not found" {
    //                 use! db = Db.buildDb ()
    //                 do! loadDocs ()
    //     
    //                 let! docs =
    //                     Custom.List(
    //                         $"SELECT data FROM {Db.tableName} WHERE data ->> 'NumValue' > @value",
    //                         [ SqliteParameter("@value", 100) ],
    //                         FromData<JsonDocument>)
    //                 Expect.isEmpty docs "There should have been no documents returned"
    //             }
    //         ]
    //         testList "NonQuery" [
    //             testTask "succeeds when operating on data" {
    //                 use! db = Db.buildDb ()
    //                 do! loadDocs ()
    //
    //                 do! Custom.NonQuery($"DELETE FROM {Db.tableName}", [])
    //
    //                 let! remaining = Count.All Db.tableName
    //                 Expect.equal remaining 0L "There should be no documents remaining in the table"
    //             }
    //             testTask "succeeds when no data matches where clause" {
    //                 use! db = Db.buildDb ()
    //                 do! loadDocs ()
    //
    //                 do! Custom.NonQuery(
    //                         $"DELETE FROM {Db.tableName} WHERE data ->> 'NumValue' > @value",
    //                         [ SqliteParameter("@value", 100) ])
    //
    //                 let! remaining = Count.All Db.tableName
    //                 Expect.equal remaining 5L "There should be 5 documents remaining in the table"
    //             }
    //         ]
    //         testTask "Scalar succeeds" {
    //             use! db = Db.buildDb ()
    //     
    //             let! nbr = Custom.Scalar("SELECT 5 AS test_value", [], System.Func<SqliteDataReader, int> _.GetInt32(0))
    //             Expect.equal nbr 5 "The query should have returned the number 5"
    //         }
    //     ]
    //     testList "Extensions" [
    //         testTask "EnsureTable succeeds" {
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
    //             do! Definition.EnsureTable "ensured"
    //             let! exists'     = itExists "ensured"
    //             let! alsoExists' = itExists "idx_ensured_key"
    //             Expect.isTrue exists'    "The table should now exist"
    //             Expect.isTrue alsoExists' "The key index should now exist"
    //         }
    //         testList "Insert" [
    //             testTask "succeeds" {
    //                 use! db     = Db.buildDb ()
    //                 use  conn   = Configuration.DbConn()
    //                 let! before = conn.FindAll<SubDocument> Db.tableName
    //                 Expect.hasCountOf before 0u isTrue "There should be no documents in the table"
    //                 do! conn.Insert(
    //                         Db.tableName,
    //                         JsonDocument(Id = "turkey", Sub = Some (SubDocument(Foo = "gobble", Bar = "gobble"))))
    //                 let! after = conn.FindAll<JsonDocument> Db.tableName
    //                 Expect.hasCountOf after 1u isTrue "There should have been one document inserted"
    //             }
    //             testTask "fails for duplicate key" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! conn.Insert(Db.tableName, JsonDocument(Id = "test"))
    //                 Expect.throws
    //                     (fun () ->
    //                         conn.Insert(Db.tableName, JsonDocument(Id = "test"))
    //                         |> Async.AwaitTask
    //                         |> Async.RunSynchronously)
    //                     "An exception should have been raised for duplicate document ID insert"
    //             }
    //         ]
    //         testList "Save" [
    //             testTask "succeeds when a document is inserted" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 let! before = conn.FindAll<JsonDocument> Db.tableName
    //                 Expect.hasCountOf before 0u isTrue "There should be no documents in the table"
    //         
    //                 do! conn.Save(
    //                         Db.tableName,
    //                         JsonDocument(Id = "test", Sub = Some (SubDocument(Foo = "a", Bar = "b"))))
    //                 let! after = conn.FindAll<JsonDocument> Db.tableName
    //                 Expect.hasCountOf after 1u isTrue "There should have been one document inserted"
    //             }
    //             testTask "succeeds when a document is updated" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! conn.Insert(
    //                         Db.tableName,
    //                         JsonDocument(Id = "test", Sub = Some (SubDocument(Foo = "a", Bar = "b"))))
    //         
    //                 let! before = conn.FindById<string, JsonDocument>(Db.tableName, "test")
    //                 if isNull before then Expect.isTrue false "There should have been a document returned"
    //                 let before = before :> JsonDocument
    //                 Expect.equal before.Id "test" "The document is not correct"
    //                 Expect.isSome before.Sub "There should have been a sub-document"
    //                 Expect.equal before.Sub.Value.Foo "a" "The document is not correct"
    //                 Expect.equal before.Sub.Value.Bar "b" "The document is not correct"
    //         
    //                 do! Save(Db.tableName, JsonDocument(Id = "test"))
    //                 let! after = conn.FindById<string, JsonDocument>(Db.tableName, "test")
    //                 if isNull after then Expect.isTrue false "There should have been a document returned post-update"
    //                 let after = after :> JsonDocument
    //                 Expect.equal after.Id "test" "The updated document is not correct"
    //                 Expect.isNone after.Sub "There should not have been a sub-document in the updated document"
    //             }
    //         ]
    //         testTask "CountAll succeeds" {
    //             use! db   = Db.buildDb ()
    //             use  conn = Configuration.DbConn()
    //             do! loadDocs ()
    //     
    //             let! theCount = conn.CountAll Db.tableName
    //             Expect.equal theCount 5L "There should have been 5 matching documents"
    //         }
    //         testTask "CountByField succeeds" {
    //             use! db   = Db.buildDb ()
    //             use  conn = Configuration.DbConn()
    //             do! loadDocs ()
    //     
    //             let! theCount = conn.CountByField(Db.tableName, "Value", Op.EQ, "purple")
    //             Expect.equal theCount 2L "There should have been 2 matching documents"
    //         }
    //         testList "ExistsById" [
    //             testTask "succeeds when a document exists" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //     
    //                 let! exists = conn.ExistsById(Db.tableName, "three")
    //                 Expect.isTrue exists "There should have been an existing document"
    //             }
    //             testTask "succeeds when a document does not exist" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //     
    //                 let! exists = conn.ExistsById(Db.tableName, "seven")
    //                 Expect.isFalse exists "There should not have been an existing document"
    //             }
    //         ]
    //         testList "ExistsByField" [
    //             testTask "succeeds when documents exist" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //     
    //                 let! exists = conn.ExistsByField(Db.tableName, "NumValue", Op.GE, 10)
    //                 Expect.isTrue exists "There should have been existing documents"
    //             }
    //             testTask "succeeds when no matching documents exist" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //     
    //                 let! exists = conn.ExistsByField(Db.tableName, "Nothing", Op.EQ, "none")
    //                 Expect.isFalse exists "There should not have been any existing documents"
    //             }
    //         ]
    //         testList "FindAll" [
    //             testTask "succeeds when there is data" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //     
    //                 do! conn.Insert(Db.tableName, JsonDocument(Id = "one", Value = "two"))
    //                 do! conn.Insert(Db.tableName, JsonDocument(Id = "three", Value = "four"))
    //                 do! conn.Insert(Db.tableName, JsonDocument(Id = "five", Value = "six"))
    //     
    //                 let! results = conn.FindAll<SubDocument> Db.tableName
    //                 Expect.hasCountOf results 3u isTrue "There should have been 3 documents returned"
    //             }
    //             testTask "succeeds when there is no data" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 let! results = conn.FindAll<SubDocument> Db.tableName
    //                 Expect.hasCountOf results 0u isTrue "There should have been no documents returned"
    //             }
    //         ]
    //         testList "FindById" [
    //             testTask "succeeds when a document is found" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //     
    //                 let! doc = conn.FindById<string, JsonDocument>(Db.tableName, "two")
    //                 if isNull doc then Expect.isTrue false "There should have been a document returned"
    //                 Expect.equal (doc :> JsonDocument).Id "two" "The incorrect document was returned"
    //             }
    //             testTask "succeeds when a document is not found" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //     
    //                 let! doc = conn.FindById<string, JsonDocument>(Db.tableName, "three hundred eighty-seven")
    //                 Expect.isNull doc "There should not have been a document returned"
    //             }
    //         ]
    //         testList "FindByField" [
    //             testTask "succeeds when documents are found" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //     
    //                 let! docs = conn.FindByField<JsonDocument>(Db.tableName, "NumValue", Op.GT, 15)
    //                 Expect.hasCountOf docs 2u isTrue "There should have been two documents returned"
    //             }
    //             testTask "succeeds when documents are not found" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //     
    //                 let! docs = conn.FindByField<JsonDocument>(Db.tableName, "Value", Op.EQ, "mauve")
    //                 Expect.hasCountOf docs 0u isTrue "There should have been no documents returned"
    //             }
    //         ]
    //         testList "FindFirstByField" [
    //             testTask "succeeds when a document is found" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //     
    //                 let! doc = conn.FindFirstByField<JsonDocument>(Db.tableName, "Value", Op.EQ, "another")
    //                 if isNull doc then Expect.isTrue false "There should have been a document returned"
    //                 Expect.equal (doc :> JsonDocument).Id "two" "The incorrect document was returned"
    //             }
    //             testTask "succeeds when multiple documents are found" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //     
    //                 let! doc = conn.FindFirstByField<JsonDocument>(Db.tableName, "Sub.Foo", Op.EQ, "green")
    //                 if isNull doc then Expect.isTrue false "There should have been a document returned"
    //                 Expect.contains [ "two"; "four" ] (doc :> JsonDocument).Id "An incorrect document was returned"
    //             }
    //             testTask "succeeds when a document is not found" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //     
    //                 let! doc = conn.FindFirstByField<JsonDocument>(Db.tableName, "Value", Op.EQ, "absent")
    //                 Expect.isNull doc "There should not have been a document returned"
    //             }
    //         ]
    //         testList "UpdateFull" [
    //             testTask "succeeds when a document is updated" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //                 
    //                 let testDoc = JsonDocument(Id = "one", Sub = Some (SubDocument(Foo = "blue", Bar = "red")))
    //                 do! conn.UpdateFull(Db.tableName, "one", testDoc)
    //                 let! after = conn.FindById<string, JsonDocument>(Db.tableName, "one")
    //                 if isNull after then Expect.isTrue false "There should have been a document returned post-update"
    //                 let after = after :> JsonDocument
    //                 Expect.equal after.Id "one" "The updated document is not correct"
    //                 Expect.isSome after.Sub "The updated document should have had a sub-document"
    //                 Expect.equal after.Sub.Value.Foo "blue" "The updated sub-document is not correct"
    //                 Expect.equal after.Sub.Value.Bar "red" "The updated sub-document is not correct"
    //             }
    //             testTask "succeeds when no document is updated" {
    //                 use! db     = Db.buildDb ()
    //                 use  conn   = Configuration.DbConn()
    //                 let! before = conn.FindAll<JsonDocument> Db.tableName
    //                 Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
    //                 
    //                 // This not raising an exception is the test
    //                 do! conn.UpdateFull(
    //                         Db.tableName,
    //                         "test",
    //                         JsonDocument(Id = "x", Sub = Some (SubDocument(Foo = "blue", Bar = "red"))))
    //             }
    //         ]
    //         testList "UpdateFullFunc" [
    //             testTask "succeeds when a document is updated" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //     
    //                 do! conn.UpdateFullFunc(
    //                     Db.tableName,
    //                     System.Func<JsonDocument, string> _.Id,
    //                     JsonDocument(Id = "one", Value = "le un", NumValue = 1, Sub = None))
    //                 let! after = conn.FindById<string, JsonDocument>(Db.tableName, "one")
    //                 if isNull after then Expect.isTrue false "There should have been a document returned post-update"
    //                 let after = after :> JsonDocument
    //                 Expect.equal after.Id "one" "The updated document is incorrect"
    //                 Expect.equal after.Value "le un" "The updated document is incorrect"
    //                 Expect.equal after.NumValue 1 "The updated document is incorrect"
    //                 Expect.isNone after.Sub "The updated document should not have a sub-document"
    //             }
    //             testTask "succeeds when no document is updated" {
    //                 use! db     = Db.buildDb ()
    //                 use  conn   = Configuration.DbConn()
    //                 let! before = conn.FindAll<JsonDocument> Db.tableName
    //                 Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
    //                 
    //                 // This not raising an exception is the test
    //                 do! conn.UpdateFullFunc(
    //                     Db.tableName,
    //                     System.Func<JsonDocument, string> _.Id,
    //                     JsonDocument(Id = "one", Value = "le un", NumValue = 1, Sub = None))
    //             }
    //         ]
    //         testList "UpdatePartialById" [
    //             testTask "succeeds when a document is updated" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //                 
    //                 do! conn.UpdatePartialById(Db.tableName, "one", {| NumValue = 44 |})
    //                 let! after = conn.FindById<string, JsonDocument>(Db.tableName, "one")
    //                 if isNull after then Expect.isTrue false "There should have been a document returned post-update"
    //                 let after = after :> JsonDocument
    //                 Expect.equal after.Id "one" "The updated document is not correct"
    //                 Expect.equal after.NumValue 44 "The updated document is not correct"
    //             }
    //             testTask "succeeds when no document is updated" {
    //                 use! db     = Db.buildDb ()
    //                 use  conn   = Configuration.DbConn()
    //                 let! before = conn.FindAll<SubDocument> Db.tableName
    //                 Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
    //                 
    //                 // This not raising an exception is the test
    //                 do! conn.UpdatePartialById(Db.tableName, "test", {| Foo = "green" |})
    //             }
    //         ]
    //         testList "UpdatePartialByField" [
    //             testTask "succeeds when a document is updated" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //                 
    //                 do! conn.UpdatePartialByField(Db.tableName, "Value", Op.EQ, "purple", {| NumValue = 77 |})
    //                 let! after = conn.CountByField(Db.tableName, "NumValue", Op.EQ, 77)
    //                 Expect.equal after 2L "There should have been 2 documents returned"
    //             }
    //             testTask "succeeds when no document is updated" {
    //                 use! db     = Db.buildDb ()
    //                 use  conn   = Configuration.DbConn()
    //                 let! before = conn.FindAll<SubDocument> Db.tableName
    //                 Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
    //                 
    //                 // This not raising an exception is the test
    //                 do! conn.UpdatePartialByField(Db.tableName, "Value", Op.EQ, "burgundy", {| Foo = "green" |})
    //             }
    //         ]
    //         testList "DeleteById" [
    //             testTask "succeeds when a document is deleted" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //     
    //                 do! conn.DeleteById(Db.tableName, "four")
    //                 let! remaining = conn.CountAll Db.tableName
    //                 Expect.equal remaining 4L "There should have been 4 documents remaining"
    //             }
    //             testTask "succeeds when a document is not deleted" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //     
    //                 do! conn.DeleteById(Db.tableName, "thirty")
    //                 let! remaining = conn.CountAll Db.tableName
    //                 Expect.equal remaining 5L "There should have been 5 documents remaining"
    //             }
    //         ]
    //         testList "DeleteByField" [
    //             testTask "succeeds when documents are deleted" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //     
    //                 do! conn.DeleteByField(Db.tableName, "Value", Op.NE, "purple")
    //                 let! remaining = conn.CountAll Db.tableName
    //                 Expect.equal remaining 2L "There should have been 2 documents remaining"
    //             }
    //             testTask "succeeds when documents are not deleted" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //     
    //                 do! conn.DeleteByField(Db.tableName, "Value", Op.EQ, "crimson")
    //                 let! remaining = Count.All Db.tableName
    //                 Expect.equal remaining 5L "There should have been 5 documents remaining"
    //             }
    //         ]
    //         testList "CustomSingle" [
    //             testTask "succeeds when a row is found" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //     
    //                 let! doc =
    //                     conn.CustomSingle(
    //                         $"SELECT data FROM {Db.tableName} WHERE data ->> 'Id' = @id",
    //                         [ SqliteParameter("@id", "one") ],
    //                         FromData<JsonDocument>)
    //                 if isNull doc then Expect.isTrue false "There should have been a document returned"
    //                 Expect.equal (doc :> JsonDocument).Id "one" "The incorrect document was returned"
    //             }
    //             testTask "succeeds when a row is not found" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //     
    //                 let! doc =
    //                     conn.CustomSingle(
    //                         $"SELECT data FROM {Db.tableName} WHERE data ->> 'Id' = @id",
    //                         [ SqliteParameter("@id", "eighty") ],
    //                         FromData<JsonDocument>)
    //                 Expect.isNull doc "There should not have been a document returned"
    //             }
    //         ]
    //         testList "CustomList" [
    //             testTask "succeeds when data is found" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //     
    //                 let! docs = conn.CustomList(Query.SelectFromTable Db.tableName, [], FromData<JsonDocument>)
    //                 Expect.hasCountOf docs 5u isTrue "There should have been 5 documents returned"
    //             }
    //             testTask "succeeds when data is not found" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //     
    //                 let! docs =
    //                     conn.CustomList(
    //                         $"SELECT data FROM {Db.tableName} WHERE data ->> 'NumValue' > @value",
    //                         [ SqliteParameter("@value", 100) ],
    //                         FromData<JsonDocument>)
    //                 Expect.isEmpty docs "There should have been no documents returned"
    //             }
    //         ]
    //         testList "CustomNonQuery" [
    //             testTask "succeeds when operating on data" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //
    //                 do! conn.CustomNonQuery($"DELETE FROM {Db.tableName}", [])
    //
    //                 let! remaining = conn.CountAll Db.tableName
    //                 Expect.equal remaining 0L "There should be no documents remaining in the table"
    //             }
    //             testTask "succeeds when no data matches where clause" {
    //                 use! db   = Db.buildDb ()
    //                 use  conn = Configuration.DbConn()
    //                 do! loadDocs ()
    //
    //                 do! conn.CustomNonQuery(
    //                         $"DELETE FROM {Db.tableName} WHERE data ->> 'NumValue' > @value",
    //                         [ SqliteParameter("@value", 100) ])
    //
    //                 let! remaining = conn.CountAll Db.tableName
    //                 Expect.equal remaining 5L "There should be 5 documents remaining in the table"
    //             }
    //         ]
    //         testTask "CustomScalar succeeds" {
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
