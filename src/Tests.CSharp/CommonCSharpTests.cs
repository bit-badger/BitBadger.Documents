namespace BitBadger.Documents.Tests.CSharp;

using Documents;
using Expecto.CSharp;
using Expecto;

/// <summary>
/// A test serializer that returns known values
/// </summary>
internal class TestSerializer : IDocumentSerializer
{
    public string Serialize<T>(T it) => "{\"Overridden\":true}";
    public T Deserialize<T>(string it) => default!;
}

/// <summary>
/// C# Tests for common functionality in <tt>BitBadger.Documents</tt>
/// </summary>
public static class CommonCSharpTests
{
    /// <summary>
    /// Unit tests
    /// </summary>
    [Tests] public static Test Unit =
        Runner.TestList("Common.C# Unit", new[]
        {
            Runner.TestSequenced(
                Runner.TestList("Configuration", new[]
                {
                    Runner.TestCase("UseSerializer succeeds", () =>
                    {
                        try
                        {
                            Configuration.UseSerializer(new TestSerializer());

                            var serialized = Configuration.Serializer().Serialize(new SubDocument
                            {
                                Foo = "howdy",
                                Bar = "bye"
                            });
                            Expect.equal(serialized, "{\"Overridden\":true}", "Specified serializer was not used");

                            var deserialized = Configuration.Serializer()
                                .Deserialize<object>("{\"Something\":\"here\"}");
                            Expect.isNull(deserialized, "Specified serializer should have returned null");
                        }
                        finally
                        {
                            Configuration.UseSerializer(DocumentSerializer.Default);
                        }
                    }),
                    Runner.TestCase("Serializer returns configured serializer", () =>
                    {
                        Expect.isTrue(ReferenceEquals(DocumentSerializer.Default, Configuration.Serializer()),
                            "Serializer should have been the same");
                    }),
                    Runner.TestCase("UseIdField / IdField succeeds", () =>
                    {
                        try
                        {
                            Expect.equal(Configuration.IdField(), "Id",
                                "The default configured ID field was incorrect");
                            Configuration.UseIdField("id");
                            Expect.equal(Configuration.IdField(), "id", "UseIdField did not set the ID field");
                        }
                        finally
                        {
                            Configuration.UseIdField("Id");
                        }
                    })
            })),
            Runner.TestList("Op", new[]
            {
                Runner.TestCase("EQ succeeds", () =>
                {
                    Expect.equal(Op.EQ.ToString(), "=", "The equals operator was not correct");
                }),
                Runner.TestCase("GT succeeds", () =>
                {
                    Expect.equal(Op.GT.ToString(), ">", "The greater than operator was not correct");
                }),
                Runner.TestCase("GE succeeds", () =>
                {
                    Expect.equal(Op.GE.ToString(), ">=", "The greater than or equal to operator was not correct");
                }),
                Runner.TestCase("LT succeeds", () =>
                {
                    Expect.equal(Op.LT.ToString(), "<", "The less than operator was not correct");
                }),
                Runner.TestCase("LE succeeds", () =>
                {
                    Expect.equal(Op.LE.ToString(), "<=", "The less than or equal to operator was not correct");
                }),
                Runner.TestCase("NE succeeds", () =>
                {
                    Expect.equal(Op.NE.ToString(), "<>", "The not equal to operator was not correct");
                }),
                Runner.TestCase("EX succeeds", () =>
                {
                    Expect.equal(Op.EX.ToString(), "IS NOT NULL", "The \"exists\" operator was not correct");
                }),
                Runner.TestCase("NEX succeeds", () =>
                {
                    Expect.equal(Op.NEX.ToString(), "IS NULL", "The \"not exists\" operator was not correct");
                })
            }),
            Runner.TestList("Query", new[]
            {
                Runner.TestCase("SelectFromTable succeeds", () =>
                {
                    Expect.equal(Query.SelectFromTable("test.table"), "SELECT data FROM test.table",
                        "SELECT statement not correct");
                }),
                Runner.TestCase("WhereById succeeds", () =>
                {
                    Expect.equal(Query.WhereById("@id"), "data ->> 'Id' = @id", "WHERE clause not correct");
                }),
                Runner.TestList("WhereByField", new[]
                {
                    Runner.TestCase("succeeds when a logical operator is passed", () =>
                    {
                        Expect.equal(Query.WhereByField("theField", Op.GT, "@test"), "data ->> 'theField' > @test",
                            "WHERE clause not correct");
                    }),
                    Runner.TestCase("succeeds when an existence operator is passed", () =>
                    {
                        Expect.equal(Query.WhereByField("thatField", Op.NEX, ""), "data ->> 'thatField' IS NULL",
                            "WHERE clause not correct");
                    })
                }),
                Runner.TestList("Definition", new[]
                {
                    Runner.TestCase("EnsureTableFor succeeds", () =>
                    {
                        Expect.equal(Query.Definition.EnsureTableFor("my.table", "JSONB"),
                            "CREATE TABLE IF NOT EXISTS my.table (data JSONB NOT NULL)",
                            "CREATE TABLE statement not constructed correctly");
                    }),
                    Runner.TestList("EnsureKey", new[]
                    {
                        Runner.TestCase("succeeds when a schema is present", () =>
                        {
                            Expect.equal(Query.Definition.EnsureKey("test.table"),
                                "CREATE UNIQUE INDEX IF NOT EXISTS idx_table_key ON test.table ((data ->> 'Id'))",
                                "CREATE INDEX for key statement with schema not constructed correctly");
                        }),
                        Runner.TestCase("succeeds when a schema is not present", () =>
                        {
                            Expect.equal(Query.Definition.EnsureKey("table"),
                                "CREATE UNIQUE INDEX IF NOT EXISTS idx_table_key ON table ((data ->> 'Id'))",
                                "CREATE INDEX for key statement without schema not constructed correctly");
                        })
                    }),
                    Runner.TestCase("EnsureIndexOn succeeds for multiple fields and directions", () =>
                    {
                        Expect.equal(
                            Query.Definition.EnsureIndexOn("test.table", "gibberish",
                                new[] { "taco", "guac DESC", "salsa ASC" }),
                            "CREATE INDEX IF NOT EXISTS idx_table_gibberish ON test.table "
                                + "((data ->> 'taco'), (data ->> 'guac') DESC, (data ->> 'salsa') ASC)",
                            "CREATE INDEX for multiple field statement incorrect");
                    })
                }),
                Runner.TestCase("Insert succeeds", () =>
                {
                    Expect.equal(Query.Insert("tbl"), "INSERT INTO tbl VALUES (@data)", "INSERT statement not correct");
                }),
                Runner.TestCase("Save succeeds", () =>
                {
                    Expect.equal(Query.Save("tbl"),
                        $"INSERT INTO tbl VALUES (@data) ON CONFLICT ((data ->> 'Id')) DO UPDATE SET data = EXCLUDED.data",
                        "INSERT ON CONFLICT UPDATE statement not correct");
                }),
                Runner.TestList("Count", new[]
                {
                    Runner.TestCase("All succeeds", () =>
                    {
                        Expect.equal(Query.Count.All("tbl"), "SELECT COUNT(*) AS it FROM tbl",
                            "Count query not correct");
                    }),
                    Runner.TestCase("ByField succeeds", () =>
                    {
                        Expect.equal(Query.Count.ByField("tbl", "thatField", Op.EQ),
                            "SELECT COUNT(*) AS it FROM tbl WHERE data ->> 'thatField' = @field",
                            "JSON field text comparison count query not correct");
                    })
                }),
                Runner.TestList("Exists", new[]
                {
                    Runner.TestCase("ById succeeds", () =>
                    {
                        Expect.equal(Query.Exists.ById("tbl"),
                            "SELECT EXISTS (SELECT 1 FROM tbl WHERE data ->> 'Id' = @id) AS it",
                            "ID existence query not correct");
                    }),
                    Runner.TestCase("ByField succeeds", () =>
                    {
                        Expect.equal(Query.Exists.ByField("tbl", "Test", Op.LT),
                            "SELECT EXISTS (SELECT 1 FROM tbl WHERE data ->> 'Test' < @field) AS it",
                            "JSON field text comparison exists query not correct");
                    })
                }),
                Runner.TestList("Find", new[]
                {
                    Runner.TestCase("ById succeeds", () =>
                    {
                        Expect.equal(Query.Find.ById("tbl"), "SELECT data FROM tbl WHERE data ->> 'Id' = @id",
                            "SELECT by ID query not correct");
                    }),
                    Runner.TestCase("ByField succeeds", () =>
                    {
                        Expect.equal(Query.Find.ByField("tbl", "Golf", Op.GE),
                            "SELECT data FROM tbl WHERE data ->> 'Golf' >= @field",
                            "SELECT by JSON comparison query not correct");
                    })
                }),
                Runner.TestList("Update", new[]
                {
                    Runner.TestCase("Full succeeds", () =>
                    {
                        Expect.equal(Query.Update.Full("tbl"), "UPDATE tbl SET data = @data WHERE data ->> 'Id' = @id",
                            "UPDATE full statement not correct");
                    }),
                    Runner.TestCase("PartialById succeeds", () =>
                    {
                        Expect.equal(Query.Update.PartialById("tbl"),
                            "UPDATE tbl SET data = json_patch(data, json(@data)) WHERE data ->> 'Id' = @id",
                            "UPDATE partial by ID statement not correct");
                    }),
                    Runner.TestCase("PartialByField succeeds", () =>
                    {
                        Expect.equal(Query.Update.PartialByField("tbl", "Part", Op.NE),
                            "UPDATE tbl SET data = json_patch(data, json(@data)) WHERE data ->> 'Part' <> @field",
                            "UPDATE partial by JSON comparison query not correct");
                    })
                }),
                Runner.TestList("Delete", new[]
                {
                    Runner.TestCase("ById succeeds", () =>
                    {
                        Expect.equal(Query.Delete.ById("tbl"), "DELETE FROM tbl WHERE data ->> 'Id' = @id",
                            "DELETE by ID query not correct");
                    }),
                    Runner.TestCase("ByField succeeds", () =>
                    {
                        Expect.equal(Query.Delete.ByField("tbl", "gone", Op.NEX),
                            "DELETE FROM tbl WHERE data ->> 'gone' IS NULL",
                            "DELETE by JSON comparison query not correct");
                    })
                })
            })
        });
}
