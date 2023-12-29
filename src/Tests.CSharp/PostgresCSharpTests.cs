using Expecto.CSharp;
using Expecto;
using BitBadger.Documents.Postgres;
using ThrowawayDb.Postgres;

namespace BitBadger.Documents.Tests.CSharp;

using static CommonExtensionsAndTypesForNpgsqlFSharp;
using static Runner;

/// <summary>
/// C# tests for the PostgreSQL implementation of <tt>BitBadger.Documents</tt>
/// </summary>
public class PostgresCSharpTests
{
    /// <summary>
    /// Tests which do not hit the database
    /// </summary>
    private static readonly Test Unit = TestList("Unit", new[]
    {
        TestList("Parameters", new[]
        {
            TestCase("Id succeeds", () =>
            {
                var it = Parameters.Id(88);
                Expect.equal(it.Item1, "@id", "ID parameter not constructed correctly");
                Expect.equal(it.Item2, Sql.@string("88"), "ID parameter value incorrect");
            }),
            TestCase("Json succeeds", () =>
            {
                var it = Parameters.Json("@test", new { Something = "good" });
                Expect.equal(it.Item1, "@test", "JSON parameter not constructed correctly");
                Expect.equal(it.Item2, Sql.jsonb("{\"Something\":\"good\"}"), "JSON parameter value incorrect");
            }),
            TestCase("Field succeeds", () =>
            {
                var it = Parameters.Field(242);
                Expect.equal(it.Item1, "@field", "Field parameter not constructed correctly");
                Expect.isTrue(it.Item2.IsParameter, "Field parameter value incorrect");
            }),
            TestCase("None succeeds", () =>
            {
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
                TestCase("EnsureDocumentIndex succeeds for full index", () =>
                {
                    Expect.equal(Postgres.Query.Definition.EnsureDocumentIndex("schema.tbl", DocumentIndex.Full),
                        "CREATE INDEX IF NOT EXISTS idx_tbl_document ON schema.tbl USING GIN (data)",
                        "CREATE INDEX statement not constructed correctly");
                }),
                TestCase("EnsureDocumentIndex succeeds for JSONB Path Ops index", () =>
                {
                    Expect.equal(
                        Postgres.Query.Definition.EnsureDocumentIndex(PostgresDb.TableName, DocumentIndex.Optimized),
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
            TestList("Patch", new[]
            {
                TestCase("ById succeeds", () =>
                {
                    Expect.equal(Postgres.Query.Patch.ById(PostgresDb.TableName),
                        $"UPDATE {PostgresDb.TableName} SET data = data || @data WHERE data ->> 'Id' = @id",
                        "UPDATE partial by ID statement not correct");
                }),
                TestCase("ByField succeeds", () =>
                {
                    Expect.equal(Postgres.Query.Patch.ByField(PostgresDb.TableName, "Snail", Op.LT),
                        $"UPDATE {PostgresDb.TableName} SET data = data || @data WHERE data ->> 'Snail' < @field",
                        "UPDATE partial by ID statement not correct");
                }),
                TestCase("ByContains succeeds", () =>
                {
                    Expect.equal(Postgres.Query.Patch.ByContains(PostgresDb.TableName),
                        $"UPDATE {PostgresDb.TableName} SET data = data || @data WHERE data @> @criteria",
                        "UPDATE partial by JSON containment statement not correct");
                }),
                TestCase("ByJsonPath succeeds", () =>
                {
                    Expect.equal(Postgres.Query.Patch.ByJsonPath(PostgresDb.TableName),
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

    /// <summary>
    /// Add the test documents to the database
    /// </summary>
    internal static async Task LoadDocs()
    {
        foreach (var doc in TestDocuments) await Document.Insert(SqliteDb.TableName, doc);
    }

    /// <summary>
    /// Integration tests for the PostgreSQL library
    /// </summary>
    private static readonly Test Integration = TestList("Integration", new[]
    {
        TestList("Configuration", new[]
        {
            TestCase("UseDataSource disposes existing source", () =>
            {
                using var db1 = ThrowawayDatabase.Create(PostgresDb.ConnStr.Value);
                var source = PostgresDb.MkDataSource(db1.ConnectionString);
                Postgres.Configuration.UseDataSource(source);

                using var db2 = ThrowawayDatabase.Create(PostgresDb.ConnStr.Value);
                Postgres.Configuration.UseDataSource(PostgresDb.MkDataSource(db2.ConnectionString));
                try
                {
                    _ = source.OpenConnection();
                    Expect.isTrue(false, "Data source should have been disposed");
                }
                catch (Exception)
                {
                    // This is what should have happened
                }
            }),
            TestCase("DataSource returns configured data source", () =>
            {
                using var db = ThrowawayDatabase.Create(PostgresDb.ConnStr.Value);
                var source = PostgresDb.MkDataSource(db.ConnectionString);
                Postgres.Configuration.UseDataSource(source);

                Expect.isTrue(ReferenceEquals(source, Postgres.Configuration.DataSource()),
                    "Data source should have been the same");
            })
        }),
        TestList("Custom", new[]
        {
            TestList("List", new[]
            {
                TestCase("succeeds when data is found", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var docs = await Custom.List(Query.SelectFromTable(PostgresDb.TableName), Parameters.None,
                        Results.FromData<JsonDocument>);
                    Expect.equal(docs.Count, 5, "There should have been 5 documents returned");
                }),
                TestCase("succeeds when data is not found", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var docs = await Custom.List(
                        $"SELECT data FROM {PostgresDb.TableName} WHERE data @? @path::jsonpath",
                        new[] { Tuple.Create("@path", Sql.@string("$.NumValue ? (@ > 100)")) },
                        Results.FromData<JsonDocument>);
                    Expect.isEmpty(docs, "There should have been no documents returned");
                })
            }),
            TestList("Single", new[]
            {
                TestCase("succeeds when a row is found", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var doc = await Custom.Single($"SELECT data FROM {PostgresDb.TableName} WHERE data ->> 'Id' = @id",
                        new[] { Tuple.Create("@id", Sql.@string("one")) }, Results.FromData<JsonDocument>);
                    Expect.isNotNull(doc, "There should have been a document returned");
                    Expect.equal(doc.Id, "one", "The incorrect document was returned");
                }),
                TestCase("succeeds when a row is not found", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var doc = await Custom.Single($"SELECT data FROM {PostgresDb.TableName} WHERE data ->> 'Id' = @id",
                        new[] { Tuple.Create("@id", Sql.@string("eighty")) }, Results.FromData<JsonDocument>);
                    Expect.isNull(doc, "There should not have been a document returned");
                })
            }),
            TestList("NonQuery", new[]
            {
                TestCase("succeeds when operating on data", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    await Custom.NonQuery($"DELETE FROM {PostgresDb.TableName}", Parameters.None);

                    var remaining = await Count.All(PostgresDb.TableName);
                    Expect.equal(remaining, 0, "There should be no documents remaining in the table");
                }),
                TestCase("succeeds when no data matches where clause", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    await Custom.NonQuery($"DELETE FROM {PostgresDb.TableName} WHERE data @? @path::jsonpath",
                        new[] { Tuple.Create("@path", Sql.@string("$.NumValue ? (@ > 100)")) });

                    var remaining = await Count.All(PostgresDb.TableName);
                    Expect.equal(remaining, 5, "There should be 5 documents remaining in the table");
                })
            }),
            TestCase("Scalar succeeds", async () =>
            {
                await using var db = PostgresDb.BuildDb();

                var nbr = await Custom.Scalar("SELECT 5 AS test_value", Parameters.None, row => row.@int("test_value"));
                Expect.equal(nbr, 5, "The query should have returned the number 5");
            })
        }),
        TestList("Definition", new[]
        {
            TestCase("EnsureTable succeeds", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                var tableExists = () => Custom.Scalar(
                    "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'ensured') AS it", Parameters.None,
                    Results.ToExists);
                var keyExists = () => Custom.Scalar(
                    "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'idx_ensured_key') AS it", Parameters.None,
                    Results.ToExists);

                var exists = await tableExists();
                var alsoExists = await keyExists();
                Expect.isFalse(exists, "The table should not exist already");
                Expect.isFalse(alsoExists, "The key index should not exist already");

                await Definition.EnsureTable("ensured");
                exists = await tableExists();
                alsoExists = await keyExists();
                Expect.isTrue(exists, "The table should now exist");
                Expect.isTrue(alsoExists, "The key index should now exist");
            }),
            TestCase("EnsureDocumentIndex succeeds", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                var indexExists = () => Custom.Scalar(
                    "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'idx_ensured_document') AS it",
                    Parameters.None, Results.ToExists);

                var exists = await indexExists();
                Expect.isFalse(exists, "The index should not exist already");

                await Definition.EnsureTable("ensured");
                await Definition.EnsureDocumentIndex("ensured", DocumentIndex.Optimized);
                exists = await indexExists();
                Expect.isTrue(exists, "The index should now exist");
            }),
            TestCase("EnsureFieldIndex succeeds", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                var indexExists = () => Custom.Scalar(
                    "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'idx_ensured_test') AS it", Parameters.None,
                    Results.ToExists);

                var exists = await indexExists();
                Expect.isFalse(exists, "The index should not exist already");

                await Definition.EnsureTable("ensured");
                await Definition.EnsureFieldIndex("ensured", "test", new[] { "Id", "Category" });
                exists = await indexExists();
                Expect.isTrue(exists, "The index should now exist");
            })
        }),
        TestList("Document", new[]
        {
            TestList("Insert", new[]
            {
                TestCase("succeeds", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    var before = await Count.All(PostgresDb.TableName);
                    Expect.equal(before, 0, "There should be no documents in the table");
            
                    await Document.Insert(PostgresDb.TableName,
                        new JsonDocument { Id = "turkey", Sub = new() { Foo = "gobble", Bar = "gobble" } });
                    var after = await Count.All(PostgresDb.TableName);
                    Expect.equal(after, 1, "There should have been one document inserted");
                }),
                TestCase("fails for duplicate key", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await Document.Insert(PostgresDb.TableName, new JsonDocument { Id = "test" });
                    try
                    {
                        await Document.Insert(PostgresDb.TableName, new JsonDocument { Id = "test" });
                        Expect.isTrue(false, "An exception should have been raised for duplicate document ID insert");
                    }
                    catch (Exception)
                    {
                        // This is what should have happened
                    }
                })
            }),
            TestList("Save", new[]
            {
                TestCase("succeeds when a document is inserted", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    var before = await Count.All(PostgresDb.TableName);
                    Expect.equal(before, 0, "There should be no documents in the table");
            
                    await Document.Save(PostgresDb.TableName,
                        new JsonDocument { Id = "test", Sub = new() { Foo = "a", Bar = "b" } });
                    var after = await Count.All(PostgresDb.TableName);
                    Expect.equal(after, 1, "There should have been one document inserted");
                }),
                TestCase("succeeds when a document is updated", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await Document.Insert(PostgresDb.TableName,
                        new JsonDocument { Id = "test", Sub = new() { Foo = "a", Bar = "b" } });

                    var before = await Find.ById<string, JsonDocument>(PostgresDb.TableName, "test");
                    Expect.isNotNull(before, "There should have been a document returned");
                    Expect.equal(before.Id, "test", "The document is not correct");

                    before.Sub = new() { Foo = "c", Bar = "d" };
                    await Document.Save(PostgresDb.TableName, before);
                    var after = await Find.ById<string, JsonDocument>(PostgresDb.TableName, "test");
                    Expect.isNotNull(after, "There should have been a document returned post-update");
                    Expect.equal(after.Id, "test", "The document is not correct");
                    Expect.equal(after.Sub!.Foo, "c", "The updated document is not correct");
                })
            })
        }),
        TestList("Count", new[]
        {
            TestCase("All succeeds", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await LoadDocs();

                var theCount = await Count.All(PostgresDb.TableName);
                Expect.equal(theCount, 5, "There should have been 5 matching documents");
            }),
            TestCase("ByField succeeds", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await LoadDocs();

                var theCount = await Count.ByField(PostgresDb.TableName, "Value", Op.EQ, "purple");
                Expect.equal(theCount, 2, "There should have been 2 matching documents");
            }),
            TestCase("ByContains succeeds", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await LoadDocs();

                var theCount = await Count.ByContains(PostgresDb.TableName, new { Value = "purple" });
                Expect.equal(theCount, 2, "There should have been 2 matching documents");
            }),
            TestCase("ByJsonPath succeeds", async () =>
            {
                await using var db = PostgresDb.BuildDb();
                await LoadDocs();

                var theCount = await Count.ByJsonPath(PostgresDb.TableName, "$.NumValue ? (@ > 5)");
                Expect.equal(theCount, 3, "There should have been 3 matching documents");
            })
        }),
        TestList("Exists", new[]
        {
            TestList("ById", new[]
            {
                TestCase("succeeds when a document exists", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var exists = await Exists.ById(PostgresDb.TableName, "three");
                    Expect.isTrue(exists, "There should have been an existing document");
                }),
                TestCase("succeeds when a document does not exist", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var exists = await Exists.ById(PostgresDb.TableName, "seven");
                    Expect.isFalse(exists, "There should not have been an existing document");
                })
            }),
            TestList("ByField", new[]
            {
                TestCase("succeeds when documents exist", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var exists = await Exists.ByField(PostgresDb.TableName, "Sub", Op.NEX, "");
                    Expect.isTrue(exists, "There should have been existing documents");
                }),
                TestCase("succeeds when documents do not exist", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var exists = await Exists.ByField(PostgresDb.TableName, "NumValue", Op.EQ, "six");
                    Expect.isFalse(exists, "There should not have been existing documents");
                })
            }),
            TestList("ByContains", new[]
            {
                TestCase("succeeds when documents exist", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var exists = await Exists.ByContains(PostgresDb.TableName, new { NumValue = 10 });
                    Expect.isTrue(exists, "There should have been existing documents");
                }),
                TestCase("succeeds when no matching documents exist", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var exists = await Exists.ByContains(PostgresDb.TableName, new { Nothing = "none" });
                    Expect.isFalse(exists, "There should not have been any existing documents");
                })
            }),
            TestList("ByJsonPath", new[] {
                TestCase("succeeds when documents exist", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var exists = await Exists.ByJsonPath(PostgresDb.TableName, "$.Sub.Foo ? (@ == \"green\")");
                    Expect.isTrue(exists, "There should have been existing documents");
                }),
                TestCase("succeeds when no matching documents exist", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var exists = await Exists.ByJsonPath(PostgresDb.TableName, "$.NumValue ? (@ > 1000)");
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
                    await using var db = PostgresDb.BuildDb();

                    await Document.Insert(PostgresDb.TableName, new SubDocument { Foo = "one", Bar = "two" });
                    await Document.Insert(PostgresDb.TableName, new SubDocument { Foo = "three", Bar = "four" });
                    await Document.Insert(PostgresDb.TableName, new SubDocument { Foo = "five", Bar = "six" });

                    var results = await Find.All<SubDocument>(PostgresDb.TableName);
                    Expect.equal(results.Count, 3, "There should have been 3 documents returned");
                }),
                TestCase("succeeds when there is no data", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    var results = await Find.All<SubDocument>(PostgresDb.TableName);
                    Expect.isEmpty(results, "There should have been no documents returned");
                })
            }),
            TestList("ById", new[]
            {
                TestCase("succeeds when a document is found", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var doc = await Find.ById<string, JsonDocument>(PostgresDb.TableName, "two");
                    Expect.isNotNull(doc, "There should have been a document returned");
                    Expect.equal(doc.Id, "two", "The incorrect document was returned");
                }),
                TestCase("succeeds when a document is not found", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var doc = await Find.ById<string, JsonDocument>(PostgresDb.TableName, "three hundred eighty-seven");
                    Expect.isNull(doc, "There should not have been a document returned");
                })
            }),
            TestList("ByField", new[]
            {
                TestCase("succeeds when documents are found", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var docs = await Find.ByField<JsonDocument>(PostgresDb.TableName, "Value", Op.EQ, "another");
                    Expect.equal(docs.Count, 1, "There should have been one document returned");
                }),
                TestCase("succeeds when documents are not found", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var docs = await Find.ByField<JsonDocument>(PostgresDb.TableName, "Value", Op.EQ, "mauve");
                    Expect.isEmpty(docs, "There should have been no documents returned");
                })
            }),
            TestList("ByContains", new[]
            {
                TestCase("succeeds when documents are found", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var docs = await Find.ByContains<JsonDocument>(PostgresDb.TableName,
                        new { Sub = new { Foo = "green" } });
                    Expect.equal(docs.Count, 2, "There should have been two documents returned");
                }),
                TestCase("succeeds when documents are not found", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var docs = await Find.ByContains<JsonDocument>(PostgresDb.TableName, new { Value = "mauve" });
                    Expect.isEmpty(docs, "There should have been no documents returned");
                })
            }),
            TestList("ByJsonPath", new[]
            {
                TestCase("succeeds when documents are found", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var docs = await Find.ByJsonPath<JsonDocument>(PostgresDb.TableName, "$.NumValue ? (@ < 15)");
                    Expect.equal(docs.Count, 3, "There should have been 3 documents returned");
                }),
                TestCase("succeeds when documents are not found", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var docs = await Find.ByJsonPath<JsonDocument>(PostgresDb.TableName, "$.NumValue ? (@ < 0)");
                    Expect.isEmpty(docs, "There should have been no documents returned");
                })
            }),
            TestList("FirstByField", new[]
            {
                TestCase("succeeds when a document is found", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var doc = await Find.FirstByField<JsonDocument>(PostgresDb.TableName, "Value", Op.EQ, "another");
                    Expect.isNotNull(doc, "There should have been a document returned");
                    Expect.equal(doc.Id, "two", "The incorrect document was returned");
                }),
                TestCase("succeeds when multiple documents are found", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var doc = await Find.FirstByField<JsonDocument>(PostgresDb.TableName, "Value", Op.EQ, "purple");
                    Expect.isNotNull(doc, "There should have been a document returned");
                    Expect.contains(new[] { "five", "four" }, doc.Id, "An incorrect document was returned");
                }),
                TestCase("succeeds when a document is not found", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var doc = await Find.FirstByField<JsonDocument>(PostgresDb.TableName, "Value", Op.EQ, "absent");
                    Expect.isNull(doc, "There should not have been a document returned");
                })
            }),
            TestList("FirstByContains", new[]
            {
                TestCase("succeeds when a document is found", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var doc = await Find.FirstByContains<JsonDocument>(PostgresDb.TableName, new { Value = "another" });
                    Expect.isNotNull(doc, "There should have been a document returned");
                    Expect.equal(doc.Id, "two", "The incorrect document was returned");
                }),
                TestCase("succeeds when multiple documents are found", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var doc = await Find.FirstByContains<JsonDocument>(PostgresDb.TableName,
                        new { Sub = new { Foo = "green" } });
                    Expect.isNotNull(doc, "There should have been a document returned");
                    Expect.contains(new[] { "two", "four" }, doc.Id, "An incorrect document was returned");
                }),
                TestCase("succeeds when a document is not found", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var doc = await Find.FirstByContains<JsonDocument>(PostgresDb.TableName, new { Value = "absent" });
                    Expect.isNull(doc, "There should not have been a document returned");
                })
            }),
            TestList("FirstByJsonPath", new[]
            {
                TestCase("succeeds when a document is found", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var doc = await Find.FirstByJsonPath<JsonDocument>(PostgresDb.TableName,
                        "$.Value ? (@ == \"FIRST!\")");
                    Expect.isNotNull(doc, "There should have been a document returned");
                    Expect.equal(doc.Id, "one", "The incorrect document was returned");
                }),
                TestCase("succeeds when multiple documents are found", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var doc = await Find.FirstByJsonPath<JsonDocument>(PostgresDb.TableName,
                        "$.Sub.Foo ? (@ == \"green\")");
                    Expect.isNotNull(doc, "There should have been a document returned");
                    Expect.contains(new[] { "two", "four" }, doc.Id, "An incorrect document was returned");
                }),
                TestCase("succeeds when a document is not found", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    var doc = await Find.FirstByJsonPath<JsonDocument>(PostgresDb.TableName, "$.Id ? (@ == \"nope\")");
                    Expect.isNull(doc, "There should not have been a document returned");
                })
            })
        }),
        TestList("Update", new[]
        {
            TestList("ById", new[]
            {
                TestCase("succeeds when a document is updated", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();
        
                    await Update.ById(PostgresDb.TableName, "one",
                        new JsonDocument { Id = "one", Sub = new() { Foo = "blue", Bar = "red" } });
                    var after = await Find.ById<string, JsonDocument>(PostgresDb.TableName, "one");
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

                    var before = await Count.All(PostgresDb.TableName);
                    Expect.equal(before, 0, "There should have been no documents returned");
                    
                    // This not raising an exception is the test
                    await Update.ById(PostgresDb.TableName, "test",
                        new JsonDocument { Id = "x", Sub = new() { Foo = "blue", Bar = "red" } });
                })
            }),
            TestList("ByFunc", new[]
            {
                TestCase("succeeds when a document is updated", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    await Update.ByFunc(PostgresDb.TableName, doc => doc.Id,
                        new JsonDocument { Id = "one", Value = "le un", NumValue = 1 });
                    var after = await Find.ById<string, JsonDocument>(PostgresDb.TableName, "one");
                    Expect.isNotNull(after, "There should have been a document returned post-update");
                    Expect.equal(after.Id, "one", "The updated document is not correct (ID)");
                    Expect.equal(after.Value, "le un", "The updated document is not correct (Value)");
                    Expect.equal(after.NumValue, 1, "The updated document is not correct (NumValue)");
                    Expect.isNull(after.Sub, "The updated document should not have had a sub-document");
                }),
                TestCase("succeeds when no document is updated", async () =>
                {
                    await using var db = PostgresDb.BuildDb();

                    var before = await Count.All(PostgresDb.TableName);
                    Expect.equal(before, 0, "There should have been no documents returned");
                    
                    // This not raising an exception is the test
                    await Update.ByFunc(PostgresDb.TableName, doc => doc.Id,
                        new JsonDocument { Id = "one", Value = "le un", NumValue = 1 });
                })
            })
        }),
        TestList("Patch", new[]
        {
            TestList("ById", new[]
            {
                TestCase("succeeds when a document is updated", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    await Patch.ById(PostgresDb.TableName, "one", new { NumValue = 44 });
                    var after = await Find.ById<string, JsonDocument>(PostgresDb.TableName, "one");
                    Expect.isNotNull(after, "There should have been a document returned post-update");
                    Expect.equal(after.NumValue, 44, "The updated document is not correct");
                }),
                TestCase("succeeds when no document is updated", async () =>
                {
                    await using var db = PostgresDb.BuildDb();

                    var before = await Count.All(PostgresDb.TableName);
                    Expect.equal(before, 0, "There should have been no documents returned");
                    
                    // This not raising an exception is the test
                    await Patch.ById(PostgresDb.TableName, "test", new { Foo = "green" });
                })
            }),
            TestList("ByField", new[]
            {
                TestCase("succeeds when a document is updated", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    await Patch.ByField(PostgresDb.TableName, "Value", Op.EQ, "purple", new { NumValue = 77 });
                    var after = await Count.ByField(PostgresDb.TableName, "NumValue", Op.EQ, "77");
                    Expect.equal(after, 2, "There should have been 2 documents returned");
                }),
                TestCase("succeeds when no document is updated", async () =>
                {
                    await using var db = PostgresDb.BuildDb();

                    var before = await Count.All(PostgresDb.TableName);
                    Expect.equal(before, 0, "There should have been no documents returned");
                    
                    // This not raising an exception is the test
                    await Patch.ByField(PostgresDb.TableName, "Value", Op.EQ, "burgundy", new { Foo = "green" });
                })
            }),
            TestList("ByContains", new[]
            {
                TestCase("succeeds when a document is updated", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    await Patch.ByContains(PostgresDb.TableName, new { Value = "purple" }, new { NumValue = 77 });
                    var after = await Count.ByContains(PostgresDb.TableName, new { NumValue = 77 });
                    Expect.equal(after, 2, "There should have been 2 documents returned");
                }),
                TestCase("succeeds when no document is updated", async () =>
                {
                    await using var db = PostgresDb.BuildDb();

                    var before = await Count.All(PostgresDb.TableName);
                    Expect.equal(before, 0, "There should have been no documents returned");
                    
                    // This not raising an exception is the test
                    await Patch.ByContains(PostgresDb.TableName, new { Value = "burgundy" }, new { Foo = "green" });
                })
            }),
            TestList("ByJsonPath", new[]
            {
                TestCase("succeeds when a document is updated", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    await Patch.ByJsonPath(PostgresDb.TableName, "$.NumValue ? (@ > 10)", new { NumValue = 1000 });
                    var after = await Count.ByJsonPath(PostgresDb.TableName, "$.NumValue ? (@ > 999)");
                    Expect.equal(after, 2, "There should have been 2 documents returned");
                }),
                TestCase("succeeds when no document is updated", async () =>
                {
                    await using var db = PostgresDb.BuildDb();

                    var before = await Count.All(PostgresDb.TableName);
                    Expect.equal(before, 0, "There should have been no documents returned");
                    
                    // This not raising an exception is the test
                    await Patch.ByJsonPath(PostgresDb.TableName, "$.NumValue ? (@ < 0)", new { Foo = "green" });
                })
            })
        }),
        TestList("Delete", new[]
        {
            TestList("ById", new[]
            {
                TestCase("succeeds when a document is deleted", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    await Delete.ById(PostgresDb.TableName, "four");
                    var remaining = await Count.All(PostgresDb.TableName);
                    Expect.equal(remaining, 4, "There should have been 4 documents remaining");
                }),
                TestCase("succeeds when a document is not deleted", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    await Delete.ById(PostgresDb.TableName, "thirty");
                    var remaining = await Count.All(PostgresDb.TableName);
                    Expect.equal(remaining, 5, "There should have been 5 documents remaining");
                })
            }),
            TestList("ByField", new[]
            {
                TestCase("succeeds when documents are deleted", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    await Delete.ByField(PostgresDb.TableName, "Value", Op.EQ, "purple");
                    var remaining = await Count.All(PostgresDb.TableName);
                    Expect.equal(remaining, 3, "There should have been 3 documents remaining");
                }),
                TestCase("succeeds when documents are not deleted", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    await Delete.ByField(PostgresDb.TableName, "Value", Op.EQ, "crimson");
                    var remaining = await Count.All(PostgresDb.TableName);
                    Expect.equal(remaining, 5, "There should have been 5 documents remaining");
                })
            }),
            TestList("ByContains", new[]
            {
                TestCase("succeeds when documents are deleted", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    await Delete.ByContains(PostgresDb.TableName, new { Value = "purple" });
                    var remaining = await Count.All(PostgresDb.TableName);
                    Expect.equal(remaining, 3, "There should have been 3 documents remaining");
                }),
                TestCase("succeeds when documents are not deleted", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    await Delete.ByContains(PostgresDb.TableName, new { Value = "crimson" });
                    var remaining = await Count.All(PostgresDb.TableName);
                    Expect.equal(remaining, 5, "There should have been 5 documents remaining");
                })
            }),
            TestList("ByJsonPath", new[]
            {
                TestCase("succeeds when documents are deleted", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    await Delete.ByJsonPath(PostgresDb.TableName, "$.Sub.Foo ? (@ == \"green\")");
                    var remaining = await Count.All(PostgresDb.TableName);
                    Expect.equal(remaining, 3, "There should have been 3 documents remaining");
                }),
                TestCase("succeeds when documents are not deleted", async () =>
                {
                    await using var db = PostgresDb.BuildDb();
                    await LoadDocs();

                    await Delete.ByJsonPath(PostgresDb.TableName, "$.NumValue ? (@ > 100)");
                    var remaining = await Count.All(PostgresDb.TableName);
                    Expect.equal(remaining, 5, "There should have been 5 documents remaining");
                })
            })
        })
    });
    
    /// <summary>
    /// All Postgres C# tests
    /// </summary>
    [Tests]
    public static readonly Test All = TestList("Postgres.C#", new[] { Unit, TestSequenced(Integration) });
}
