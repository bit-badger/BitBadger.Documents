module PostgresTests

open Expecto
open BitBadger.Documents
open BitBadger.Documents.Postgres
open BitBadger.Documents.Tests

/// Tests which do not hit the database
let unitTests =
    testList "Unit" [
        testList "Definition" [
            test "createTable succeeds" {
                Expect.equal (Definition.createTable PostgresDb.TableName)
                    $"CREATE TABLE IF NOT EXISTS {PostgresDb.TableName} (data JSONB NOT NULL)"
                    "CREATE TABLE statement not constructed correctly"
            }
            test "createKey succeeds" {
                Expect.equal (Definition.createKey PostgresDb.TableName)
                    $"CREATE UNIQUE INDEX IF NOT EXISTS idx_{PostgresDb.TableName}_key ON {PostgresDb.TableName} ((data ->> 'Id'))"
                    "CREATE INDEX for key statement not constructed correctly"
            }
            test "createIndex succeeds for full index" {
                Expect.equal (Definition.createIndex "schema.tbl" Full)
                    "CREATE INDEX IF NOT EXISTS idx_tbl ON schema.tbl USING GIN (data)"
                    "CREATE INDEX statement not constructed correctly"
            }
            test "createIndex succeeds for JSONB Path Ops index" {
                Expect.equal (Definition.createIndex PostgresDb.TableName Optimized)
                    $"CREATE INDEX IF NOT EXISTS idx_{PostgresDb.TableName} ON {PostgresDb.TableName} USING GIN (data jsonb_path_ops)"
                    "CREATE INDEX statement not constructed correctly"
            }
        ]
        testList "Query" [
            test "whereDataContains succeeds" {
                Expect.equal (Query.whereDataContains "@test") "data @> @test" "WHERE clause not correct"
            }
            test "whereJsonPathMatches succeeds" {
                Expect.equal (Query.whereJsonPathMatches "@path") "data @? @path::jsonpath" "WHERE clause not correct"
            }
            test "jsonbDocParam succeeds" {
                Expect.equal (Query.jsonbDocParam {| Hello = "There" |}) (Sql.jsonb "{\"Hello\":\"There\"}")
                    "JSONB document not serialized correctly"
            }
            test "docParameters succeeds" {
                let parameters = Query.docParameters "abc123" {| Testing = 456 |}
                let expected = [
                    "@id", Sql.string "abc123"
                    "@data", Sql.jsonb "{\"Testing\":456}"
                ]
                Expect.equal parameters expected "There should have been 2 parameters, one string and one JSONB"
            }
            test "insert succeeds" {
                Expect.equal (Query.insert PostgresDb.TableName) $"INSERT INTO {PostgresDb.TableName} VALUES (@data)"
                    "INSERT statement not correct"
            }
            test "save succeeds" {
                Expect.equal (Query.save PostgresDb.TableName)
                    $"INSERT INTO {PostgresDb.TableName} VALUES (@data) ON CONFLICT ((data ->> 'Id')) DO UPDATE SET data = EXCLUDED.data"
                    "INSERT ON CONFLICT UPDATE statement not correct"
            }
            testList "Count" [
                test "byContains succeeds" {
                    Expect.equal (Query.Count.byContains PostgresDb.TableName)
                        $"SELECT COUNT(*) AS it FROM {PostgresDb.TableName} WHERE data @> @criteria"
                        "JSON containment count query not correct"
                }
                test "byJsonPath succeeds" {
                    Expect.equal (Query.Count.byJsonPath PostgresDb.TableName)
                        $"SELECT COUNT(*) AS it FROM {PostgresDb.TableName} WHERE data @? @path::jsonpath"
                        "JSON Path match count query not correct"
                }
            ]
            testList "Exists" [
                test "byContains succeeds" {
                    Expect.equal (Query.Exists.byContains PostgresDb.TableName)
                        $"SELECT EXISTS (SELECT 1 FROM {PostgresDb.TableName} WHERE data @> @criteria) AS it"
                        "JSON containment exists query not correct"
                }
                test "byJsonPath succeeds" {
                    Expect.equal (Query.Exists.byJsonPath PostgresDb.TableName)
                        $"SELECT EXISTS (SELECT 1 FROM {PostgresDb.TableName} WHERE data @? @path::jsonpath) AS it"
                        "JSON Path match existence query not correct"
                }
            ]
            testList "Find" [
                test "byContains succeeds" {
                    Expect.equal (Query.Find.byContains PostgresDb.TableName)
                        $"SELECT data FROM {PostgresDb.TableName} WHERE data @> @criteria"
                        "SELECT by JSON containment query not correct"
                }
                test "byJsonPath succeeds" {
                    Expect.equal (Query.Find.byJsonPath PostgresDb.TableName)
                        $"SELECT data FROM {PostgresDb.TableName} WHERE data @? @path::jsonpath"
                        "SELECT by JSON Path match query not correct"
                }
            ]
            testList "Update" [
                test "partialById succeeds" {
                    Expect.equal (Query.Update.partialById PostgresDb.TableName)
                        $"UPDATE {PostgresDb.TableName} SET data = data || @data WHERE data ->> 'Id' = @id"
                        "UPDATE partial by ID statement not correct"
                }
                test "partialByContains succeeds" {
                    Expect.equal (Query.Update.partialByContains PostgresDb.TableName)
                        $"UPDATE {PostgresDb.TableName} SET data = data || @data WHERE data @> @criteria"
                        "UPDATE partial by JSON containment statement not correct"
                }
                test "partialByJsonPath succeeds" {
                    Expect.equal (Query.Update.partialByJsonPath PostgresDb.TableName)
                        $"UPDATE {PostgresDb.TableName} SET data = data || @data WHERE data @? @path::jsonpath"
                        "UPDATE partial by JSON Path statement not correct"
                }
            ]
            testList "Delete" [
                test "byContains succeeds" {
                    Expect.equal (Query.Delete.byContains PostgresDb.TableName)
                        $"DELETE FROM {PostgresDb.TableName} WHERE data @> @criteria"
                        "DELETE by JSON containment query not correct"
                }
                test "byJsonPath succeeds" {
                    Expect.equal (Query.Delete.byJsonPath PostgresDb.TableName)
                        $"DELETE FROM {PostgresDb.TableName} WHERE data @? @path::jsonpath"
                        "DELETE by JSON Path match query not correct"
                }
            ]
        ]
    ]

open Npgsql.FSharp
open ThrowawayDb.Postgres
open Types

let isTrue<'T> (_ : 'T) = true

let integrationTests =
    let documents = [
        { Id = "one"; Value = "FIRST!"; NumValue = 0; Sub = None }
        { Id = "two"; Value = "another"; NumValue = 10; Sub = Some { Foo = "green"; Bar = "blue" } }
        { Id = "three"; Value = ""; NumValue = 4; Sub = None }
        { Id = "four"; Value = "purple"; NumValue = 17; Sub = Some { Foo = "green"; Bar = "red" } }
        { Id = "five"; Value = "purple"; NumValue = 18; Sub = None }
    ]
    let loadDocs () = backgroundTask {
        for doc in documents do do! insert PostgresDb.TableName doc
    }
    testList "Integration" [
        testList "Configuration" [
            test "useDataSource disposes existing source" {
                use db1 = ThrowawayDatabase.Create PostgresDb.ConnStr.Value
                let source = PostgresDb.MkDataSource db1.ConnectionString
                Configuration.useDataSource source

                use db2 = ThrowawayDatabase.Create PostgresDb.ConnStr.Value
                Configuration.useDataSource (PostgresDb.MkDataSource db2.ConnectionString)
                Expect.throws (fun () -> source.OpenConnection() |> ignore) "Data source should have been disposed"
            }
            test "dataSource returns configured data source" {
                use db = ThrowawayDatabase.Create PostgresDb.ConnStr.Value
                let source = PostgresDb.MkDataSource db.ConnectionString
                Configuration.useDataSource source

                Expect.isTrue (obj.ReferenceEquals(source, Configuration.dataSource ()))
                    "Data source should have been the same"
            }
        ]
        testList "Definition" [
            testTask "ensureTable succeeds" {
                use db = PostgresDb.BuildDb()
                let tableExists () =
                    Sql.connect db.ConnectionString
                    |> Sql.query "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'ensured') AS it"
                    |> Sql.executeRowAsync (fun row -> row.bool "it")
                let keyExists () =
                    Sql.connect db.ConnectionString
                    |> Sql.query "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'idx_ensured_key') AS it"
                    |> Sql.executeRowAsync (fun row -> row.bool "it")
                
                let! exists     = tableExists ()
                let! alsoExists = keyExists ()
                Expect.isFalse exists     "The table should not exist already"
                Expect.isFalse alsoExists "The key index should not exist already"

                do! Definition.ensureTable "ensured"
                let! exists'     = tableExists ()
                let! alsoExists' = keyExists   ()
                Expect.isTrue exists'    "The table should now exist"
                Expect.isTrue alsoExists' "The key index should now exist"
            }
            testTask "ensureIndex succeeds" {
                use db = PostgresDb.BuildDb()
                let indexExists () =
                    Sql.connect db.ConnectionString
                    |> Sql.query "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'idx_ensured') AS it"
                    |> Sql.executeRowAsync (fun row -> row.bool "it")
                
                let! exists = indexExists ()
                Expect.isFalse exists "The index should not exist already"

                do! Definition.ensureTable "ensured"
                do! Definition.ensureIndex "ensured" Optimized
                let! exists' = indexExists ()
                Expect.isTrue exists' "The index should now exist"
                // TODO: check for GIN(jsonp_path_ops), write test for "full" index that checks for their absence
            }
        ]
        testList "insert" [
            testTask "succeeds" {
                use db = PostgresDb.BuildDb()
                let! before = Find.all<SubDocument> PostgresDb.TableName
                Expect.equal before [] "There should be no documents in the table"

                let testDoc = { emptyDoc with Id = "turkey"; Sub = Some { Foo = "gobble"; Bar = "gobble" } }
                do! insert PostgresDb.TableName testDoc
                let! after = Find.all<JsonDocument> PostgresDb.TableName
                Expect.equal after [ testDoc ] "There should have been one document inserted"
            }
            testTask "fails for duplicate key" {
                use db = PostgresDb.BuildDb()
                do! insert PostgresDb.TableName { emptyDoc with Id = "test" }
                Expect.throws (fun () ->
                    insert PostgresDb.TableName {emptyDoc with Id = "test" } |> Async.AwaitTask |> Async.RunSynchronously)
                    "An exception should have been raised for duplicate document ID insert"
            }
        ]
        testList "save" [
            testTask "succeeds when a document is inserted" {
                use db = PostgresDb.BuildDb()
                let! before = Find.all<JsonDocument> PostgresDb.TableName
                Expect.equal before [] "There should be no documents in the table"

                let testDoc = { emptyDoc with Id = "test"; Sub = Some { Foo = "a"; Bar = "b" } }
                do! save PostgresDb.TableName testDoc
                let! after = Find.all<JsonDocument> PostgresDb.TableName
                Expect.equal after [ testDoc ] "There should have been one document inserted"
            }
            testTask "succeeds when a document is updated" {
                use db = PostgresDb.BuildDb()
                let testDoc = { emptyDoc with Id = "test"; Sub = Some { Foo = "a"; Bar = "b" } }
                do! insert PostgresDb.TableName testDoc

                let! before = Find.byId<string, JsonDocument> PostgresDb.TableName "test"
                Expect.isSome before "There should have been a document returned"
                Expect.equal before.Value testDoc "The document is not correct"

                let upd8Doc = { testDoc with Sub = Some { Foo = "c"; Bar = "d" } }
                do! save PostgresDb.TableName upd8Doc
                let! after = Find.byId<string, JsonDocument> PostgresDb.TableName "test"
                Expect.isSome after "There should have been a document returned post-update"
                Expect.equal after.Value upd8Doc "The updated document is not correct"
            }
        ]
        testList "Count" [
            testTask "all succeeds" {
                use db = PostgresDb.BuildDb()
                do! loadDocs ()

                let! theCount = Count.all PostgresDb.TableName
                Expect.equal theCount 5 "There should have been 5 matching documents"
            }
            testTask "byContains succeeds" {
                use db = PostgresDb.BuildDb()
                do! loadDocs ()

                let! theCount = Count.byContains PostgresDb.TableName {| Value = "purple" |}
                Expect.equal theCount 2 "There should have been 2 matching documents"
            }
            testTask "byJsonPath succeeds" {
                use db = PostgresDb.BuildDb()
                do! loadDocs ()

                let! theCount = Count.byJsonPath PostgresDb.TableName "$.NumValue ? (@ > 5)"
                Expect.equal theCount 3 "There should have been 3 matching documents"
            }
        ]
        testList "Exists" [
            testList "byId" [
                testTask "succeeds when a document exists" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let! exists = Exists.byId PostgresDb.TableName "three"
                    Expect.isTrue exists "There should have been an existing document"
                }
                testTask "succeeds when a document does not exist" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let! exists = Exists.byId PostgresDb.TableName "seven"
                    Expect.isFalse exists "There should not have been an existing document"
                }
            ]
            testList "byContains" [
                testTask "succeeds when documents exist" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let! exists = Exists.byContains PostgresDb.TableName {| NumValue = 10 |}
                    Expect.isTrue exists "There should have been existing documents"
                }
                testTask "succeeds when no matching documents exist" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let! exists = Exists.byContains PostgresDb.TableName {| Nothing = "none" |}
                    Expect.isFalse exists "There should not have been any existing documents"
                }
            ]
            testList "byJsonPath" [
                testTask "succeeds when documents exist" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let! exists = Exists.byJsonPath PostgresDb.TableName """$.Sub.Foo ? (@ == "green")"""
                    Expect.isTrue exists "There should have been existing documents"
                }
                testTask "succeeds when no matching documents exist" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let! exists = Exists.byJsonPath PostgresDb.TableName "$.NumValue ? (@ > 1000)"
                    Expect.isFalse exists "There should not have been any existing documents"
                }
            ]
        ]
        testList "Find" [
            testList "all" [
                testTask "succeeds when there is data" {
                    use db = PostgresDb.BuildDb()

                    do! insert PostgresDb.TableName { Foo = "one"; Bar = "two" }
                    do! insert PostgresDb.TableName { Foo = "three"; Bar = "four" }
                    do! insert PostgresDb.TableName { Foo = "five"; Bar = "six" }

                    let! results = Find.all<SubDocument> PostgresDb.TableName
                    let expected = [
                        { Foo = "one"; Bar = "two" }
                        { Foo = "three"; Bar = "four" }
                        { Foo = "five"; Bar = "six" }
                    ]
                    Expect.equal results expected "There should have been 3 documents returned"
                }
                testTask "succeeds when there is no data" {
                    use db = PostgresDb.BuildDb()
                    let! results = Find.all<SubDocument> PostgresDb.TableName
                    Expect.equal results [] "There should have been no documents returned"
                }
            ]
            testList "byId" [
                testTask "succeeds when a document is found" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let! doc = Find.byId<string, JsonDocument> PostgresDb.TableName "two"
                    Expect.isTrue (Option.isSome doc) "There should have been a document returned"
                    Expect.equal doc.Value.Id "two" "The incorrect document was returned"
                }
                testTask "succeeds when a document is not found" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let! doc = Find.byId<string, JsonDocument> PostgresDb.TableName "three hundred eighty-seven"
                    Expect.isFalse (Option.isSome doc) "There should not have been a document returned"
                }
            ]
            testList "byContains" [
                testTask "succeeds when documents are found" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let! docs = Find.byContains<JsonDocument> PostgresDb.TableName {| Sub = {| Foo = "green" |} |}
                    Expect.equal (List.length docs) 2 "There should have been two documents returned"
                }
                testTask "succeeds when documents are not found" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let! docs = Find.byContains<JsonDocument> PostgresDb.TableName {| Value = "mauve" |}
                    Expect.isTrue (List.isEmpty docs) "There should have been no documents returned"
                }
            ]
            testList "byJsonPath" [
                testTask "succeeds when documents are found" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let! docs = Find.byJsonPath<JsonDocument> PostgresDb.TableName "$.NumValue ? (@ < 15)"
                    Expect.equal (List.length docs) 3 "There should have been 3 documents returned"
                }
                testTask "succeeds when documents are not found" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let! docs = Find.byJsonPath<JsonDocument> PostgresDb.TableName "$.NumValue ? (@ < 0)"
                    Expect.isTrue (List.isEmpty docs) "There should have been no documents returned"
                }
            ]
            testList "firstByContains" [
                testTask "succeeds when a document is found" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let! doc = Find.firstByContains<JsonDocument> PostgresDb.TableName {| Value = "another" |}
                    Expect.isTrue (Option.isSome doc) "There should have been a document returned"
                    Expect.equal doc.Value.Id "two" "The incorrect document was returned"
                }
                testTask "succeeds when multiple documents are found" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let! doc = Find.firstByContains<JsonDocument> PostgresDb.TableName {| Sub = {| Foo = "green" |} |}
                    Expect.isTrue (Option.isSome doc) "There should have been a document returned"
                    Expect.contains [ "two"; "four" ] doc.Value.Id "An incorrect document was returned"
                }
                testTask "succeeds when a document is not found" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let! doc = Find.firstByContains<JsonDocument> PostgresDb.TableName {| Value = "absent" |}
                    Expect.isFalse (Option.isSome doc) "There should not have been a document returned"
                }
            ]
            testList "firstByJsonPath" [
                testTask "succeeds when a document is found" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let! doc = Find.firstByJsonPath<JsonDocument> PostgresDb.TableName """$.Value ? (@ == "FIRST!")"""
                    Expect.isTrue (Option.isSome doc) "There should have been a document returned"
                    Expect.equal doc.Value.Id "one" "The incorrect document was returned"
                }
                testTask "succeeds when multiple documents are found" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let! doc = Find.firstByJsonPath<JsonDocument> PostgresDb.TableName """$.Sub.Foo ? (@ == "green")"""
                    Expect.isTrue (Option.isSome doc) "There should have been a document returned"
                    Expect.contains [ "two"; "four" ] doc.Value.Id "An incorrect document was returned"
                }
                testTask "succeeds when a document is not found" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let! doc = Find.firstByJsonPath<JsonDocument> PostgresDb.TableName """$.Id ? (@ == "nope")"""
                    Expect.isFalse (Option.isSome doc) "There should not have been a document returned"
                }
            ]
        ]
        testList "Update" [
            testList "full" [
                testTask "succeeds when a document is updated" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let testDoc = { emptyDoc with Id = "one"; Sub = Some { Foo = "blue"; Bar = "red" } }
                    do! Update.full PostgresDb.TableName "one" testDoc
                    let! after = Find.byId<string, JsonDocument> PostgresDb.TableName "one"
                    Expect.isSome after "There should have been a document returned post-update"
                    Expect.equal after.Value testDoc "The updated document is not correct"
                }
                testTask "succeeds when no document is updated" {
                    use db = PostgresDb.BuildDb()

                    let! before = Find.all<JsonDocument> PostgresDb.TableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.full PostgresDb.TableName "test"
                            { emptyDoc with Id = "x"; Sub = Some { Foo = "blue"; Bar = "red" } }
                }
            ]
            testList "fullFunc" [
                testTask "succeeds when a document is updated" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    do! Update.fullFunc PostgresDb.TableName (_.Id)
                            { Id = "one"; Value = "le un"; NumValue = 1; Sub = None }
                    let! after = Find.byId<string, JsonDocument> PostgresDb.TableName "one"
                    Expect.isSome after "There should have been a document returned post-update"
                    Expect.equal after.Value { Id = "one"; Value = "le un"; NumValue = 1; Sub = None }
                        "The updated document is not correct"
                }
                testTask "succeeds when no document is updated" {
                    use db = PostgresDb.BuildDb()

                    let! before = Find.all<JsonDocument> PostgresDb.TableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.fullFunc PostgresDb.TableName (_.Id) { Id = "one"; Value = "le un"; NumValue = 1; Sub = None }
                }
            ]
            testList "partialById" [
                testTask "succeeds when a document is updated" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()
                    
                    do! Update.partialById PostgresDb.TableName "one" {| NumValue = 44 |}
                    let! after = Find.byId<string, JsonDocument> PostgresDb.TableName "one"
                    Expect.isSome after "There should have been a document returned post-update"
                    Expect.equal after.Value.NumValue 44 "The updated document is not correct"
                }
                testTask "succeeds when no document is updated" {
                    use db = PostgresDb.BuildDb()

                    let! before = Find.all<SubDocument> PostgresDb.TableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.partialById PostgresDb.TableName "test" {| Foo = "green" |}
                }
            ]
            testList "partialByContains" [
                testTask "succeeds when a document is updated" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()
                    
                    do! Update.partialByContains PostgresDb.TableName {| Value = "purple" |} {| NumValue = 77 |}
                    let! after = Count.byContains PostgresDb.TableName {| NumValue = 77 |}
                    Expect.equal after 2 "There should have been 2 documents returned"
                }
                testTask "succeeds when no document is updated" {
                    use db = PostgresDb.BuildDb()

                    let! before = Find.all<SubDocument> PostgresDb.TableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.partialByContains PostgresDb.TableName {| Value = "burgundy" |} {| Foo = "green" |}
                }
            ]
            testList "partialByJsonPath" [
                testTask "succeeds when a document is updated" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()
                    
                    do! Update.partialByJsonPath PostgresDb.TableName "$.NumValue ? (@ > 10)" {| NumValue = 1000 |}
                    let! after = Count.byJsonPath PostgresDb.TableName "$.NumValue ? (@ > 999)"
                    Expect.equal after 2 "There should have been 2 documents returned"
                }
                testTask "succeeds when no document is updated" {
                    use db = PostgresDb.BuildDb()

                    let! before = Find.all<SubDocument> PostgresDb.TableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.partialByContains PostgresDb.TableName {| Value = "burgundy" |} {| Foo = "green" |}
                }
            ]
        ]
        testList "Delete" [
            testList "byId" [
                testTask "succeeds when a document is deleted" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    do! Delete.byId PostgresDb.TableName "four"
                    let! remaining = Count.all PostgresDb.TableName
                    Expect.equal remaining 4 "There should have been 4 documents remaining"
                }
                testTask "succeeds when a document is not deleted" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    do! Delete.byId PostgresDb.TableName "thirty"
                    let! remaining = Count.all PostgresDb.TableName
                    Expect.equal remaining 5 "There should have been 5 documents remaining"
                }
            ]
            testList "byContains" [
                testTask "succeeds when documents are deleted" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    do! Delete.byContains PostgresDb.TableName {| Value = "purple" |}
                    let! remaining = Count.all PostgresDb.TableName
                    Expect.equal remaining 3 "There should have been 3 documents remaining"
                }
                testTask "succeeds when documents are not deleted" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    do! Delete.byContains PostgresDb.TableName {| Value = "crimson" |}
                    let! remaining = Count.all PostgresDb.TableName
                    Expect.equal remaining 5 "There should have been 5 documents remaining"
                }
            ]
            testList "byJsonPath" [
                testTask "succeeds when documents are deleted" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    do! Delete.byJsonPath PostgresDb.TableName """$.Sub.Foo ? (@ == "green")"""
                    let! remaining = Count.all PostgresDb.TableName
                    Expect.equal remaining 3 "There should have been 3 documents remaining"
                }
                testTask "succeeds when documents are not deleted" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    do! Delete.byJsonPath PostgresDb.TableName "$.NumValue ? (@ > 100)"
                    let! remaining = Count.all PostgresDb.TableName
                    Expect.equal remaining 5 "There should have been 5 documents remaining"
                }
            ]
        ]
        testList "Custom" [
            testList "single" [
                testTask "succeeds when a row is found" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let! doc =
                        Custom.single $"SELECT data FROM {PostgresDb.TableName} WHERE data ->> 'Id' = @id"
                                      [ "@id", Sql.string "one"] fromData<JsonDocument>
                    Expect.isSome doc "There should have been a document returned"
                    Expect.equal doc.Value.Id "one" "The incorrect document was returned"
                }
                testTask "succeeds when a row is not found" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let! doc =
                        Custom.single $"SELECT data FROM {PostgresDb.TableName} WHERE data ->> 'Id' = @id"
                                      [ "@id", Sql.string "eighty" ] fromData<JsonDocument>
                    Expect.isNone doc "There should not have been a document returned"
                }
            ]
            testList "list" [
                testTask "succeeds when data is found" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let! docs = Custom.list (Query.selectFromTable PostgresDb.TableName) [] fromData<JsonDocument>
                    Expect.hasCountOf docs 5u isTrue "There should have been 5 documents returned"
                }
                testTask "succeeds when data is not found" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    let! docs =
                        Custom.list $"SELECT data FROM {PostgresDb.TableName} WHERE data @? @path::jsonpath"
                                    [ "@path", Sql.string "$.NumValue ? (@ > 100)" ] fromData<JsonDocument>
                    Expect.isEmpty docs "There should have been no documents returned"
                }
            ]
            testList "nonQuery" [
                testTask "succeeds when operating on data" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    do! Custom.nonQuery $"DELETE FROM {PostgresDb.TableName}" []

                    let! remaining = Count.all PostgresDb.TableName
                    Expect.equal remaining 0 "There should be no documents remaining in the table"
                }
                testTask "succeeds when no data matches where clause" {
                    use db = PostgresDb.BuildDb()
                    do! loadDocs ()

                    do! Custom.nonQuery $"DELETE FROM {PostgresDb.TableName} WHERE data @? @path::jsonpath"
                                        [ "@path", Sql.string "$.NumValue ? (@ > 100)" ]

                    let! remaining = Count.all PostgresDb.TableName
                    Expect.equal remaining 5 "There should be 5 documents remaining in the table"
                }
            ]
            testTask "scalar succeeds" {
                use db = PostgresDb.BuildDb()

                let! nbr = Custom.scalar $"SELECT 5 AS test_value" [] (fun row -> row.int "test_value")
                Expect.equal nbr 5 "The query should have returned the number 5"
            }
        ]
    ]
    |> testSequenced


let all = testList "FSharp.Documents" [ unitTests; integrationTests ]
