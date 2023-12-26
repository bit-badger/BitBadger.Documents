module SqliteTests

open BitBadger.Documents
open BitBadger.Documents.Sqlite
open BitBadger.Documents.Tests
open Expecto
open Microsoft.Data.Sqlite
open Types

/// Unit tests for the SQLite library
let unitTests =
    testList "Unit" [
        testList "Parameters" [
            test "idParam succeeds" {
                let theParam = idParam 7
                Expect.equal theParam.ParameterName "@id" "The parameter name is incorrect"
                Expect.equal theParam.Value "7" "The parameter value is incorrect"
            }
            test "jsonParam succeeds" {
                let theParam = jsonParam "@test" {| Nice = "job" |}
                Expect.equal theParam.ParameterName "@test" "The parameter name is incorrect"
                Expect.equal theParam.Value """{"Nice":"job"}""" "The parameter value is incorrect"
            }
            test "fieldParam succeeds" {
                let theParam = fieldParam 99
                Expect.equal theParam.ParameterName "@field" "The parameter name is incorrect"
                Expect.equal theParam.Value 99 "The parameter value is incorrect"
            }
            test "noParams succeeds" {
                Expect.isEmpty noParams "The parameter list should have been empty"
            }
        ]
        // Results are exhaustively executed in the context of other tests
    ]

/// These tests each use a fresh copy of a SQLite database
let integrationTests =
    let documents = [
        { Id = "one"; Value = "FIRST!"; NumValue = 0; Sub = None }
        { Id = "two"; Value = "another"; NumValue = 10; Sub = Some { Foo = "green"; Bar = "blue" } }
        { Id = "three"; Value = ""; NumValue = 4; Sub = None }
        { Id = "four"; Value = "purple"; NumValue = 17; Sub = Some { Foo = "green"; Bar = "red" } }
        { Id = "five"; Value = "purple"; NumValue = 18; Sub = None }
    ]
    let loadDocs () = backgroundTask {
        for doc in documents do do! insert SqliteDb.TableName doc
    }
    testList "Integration" [
        testList "Configuration" [
            test "useConnectionString / connectionString succeed" {
                try
                    Configuration.useConnectionString "Data Source=test.db"
                    Expect.equal
                        Configuration.connectionString
                        (Some "Data Source=test.db;Foreign Keys=True")
                        "Connection string incorrect"
                finally
                    Configuration.useConnectionString "Data Source=:memory:"
            }
            test "useSerializer succeeds" {
                try
                    Configuration.useSerializer
                        { new IDocumentSerializer with
                            member _.Serialize<'T>(it: 'T) : string = """{"Overridden":true}"""
                            member _.Deserialize<'T>(it: string) : 'T = Unchecked.defaultof<'T>
                        }
                    
                    let serialized = Configuration.serializer().Serialize { Foo = "howdy"; Bar = "bye"}
                    Expect.equal serialized """{"Overridden":true}""" "Specified serializer was not used"
                    
                    let deserialized = Configuration.serializer().Deserialize<obj> """{"Something":"here"}"""
                    Expect.isNull deserialized "Specified serializer should have returned null"
                finally
                    Configuration.useSerializer DocumentSerializer.``default``
            }
            test "serializer returns configured serializer" {
                Expect.isTrue (obj.ReferenceEquals(DocumentSerializer.``default``, Configuration.serializer ()))
                    "Serializer should have been the same"
            }
            test "useIdField / idField succeeds" {
                Expect.equal (Configuration.idField ()) "Id" "The default configured ID field was incorrect"
                Configuration.useIdField "id"
                Expect.equal (Configuration.idField ()) "id" "useIdField did not set the ID field"
                Configuration.useIdField "Id"
            }
        ]
        testList "Custom" [
            testList "single" [
                testTask "succeeds when a row is found" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
        
                    let! doc =
                        Custom.single
                            $"SELECT data FROM {SqliteDb.TableName} WHERE data ->> 'Id' = @id"
                            [ SqliteParameter("@id", "one") ]
                            fromData<JsonDocument>
                    Expect.isSome doc "There should have been a document returned"
                    Expect.equal doc.Value.Id "one" "The incorrect document was returned"
                }
                testTask "succeeds when a row is not found" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
        
                    let! doc =
                        Custom.single
                            $"SELECT data FROM {SqliteDb.TableName} WHERE data ->> 'Id' = @id"
                            [ SqliteParameter("@id", "eighty") ]
                            fromData<JsonDocument>
                    Expect.isNone doc "There should not have been a document returned"
                }
            ]
            testList "list" [
                testTask "succeeds when data is found" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
        
                    let! docs = Custom.list (Query.selectFromTable SqliteDb.TableName) [] fromData<JsonDocument>
                    Expect.hasCountOf docs 5u (fun _ -> true) "There should have been 5 documents returned"
                }
                testTask "succeeds when data is not found" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
        
                    let! docs =
                        Custom.list
                            $"SELECT data FROM {SqliteDb.TableName} WHERE data ->> 'NumValue' > @value"
                            [ SqliteParameter("@value", 100) ]
                            fromData<JsonDocument>
                    Expect.isEmpty docs "There should have been no documents returned"
                }
            ]
            testList "nonQuery" [
                testTask "succeeds when operating on data" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()

                    do! Custom.nonQuery $"DELETE FROM {SqliteDb.TableName}" []

                    let! remaining = Count.all SqliteDb.TableName
                    Expect.equal remaining 0L "There should be no documents remaining in the table"
                }
                testTask "succeeds when no data matches where clause" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()

                    do! Custom.nonQuery
                            $"DELETE FROM {SqliteDb.TableName} WHERE data ->> 'NumValue' > @value"
                            [ SqliteParameter("@value", 100) ]

                    let! remaining = Count.all SqliteDb.TableName
                    Expect.equal remaining 5L "There should be 5 documents remaining in the table"
                }
            ]
            testTask "scalar succeeds" {
                use! db = SqliteDb.BuildDb()
        
                let! nbr = Custom.scalar "SELECT 5 AS test_value" [] _.GetInt32(0)
                Expect.equal nbr 5 "The query should have returned the number 5"
            }
        ]
        testList "Definition" [
            testTask "ensureTable succeeds" {
                use! db = SqliteDb.BuildDb()
                let itExists (name: string) = task {
                    let! result =
                        Custom.scalar
                            $"SELECT EXISTS (SELECT 1 FROM {SqliteDb.Catalog} WHERE name = @name) AS it"
                            [ SqliteParameter("@name", name) ]
                            _.GetInt64(0)
                    return result > 0
                }
                
                let! exists     = itExists "ensured"
                let! alsoExists = itExists "idx_ensured_key"
                Expect.isFalse exists     "The table should not exist already"
                Expect.isFalse alsoExists "The key index should not exist already"
        
                do! Definition.ensureTable "ensured"
                let! exists'     = itExists "ensured"
                let! alsoExists' = itExists "idx_ensured_key"
                Expect.isTrue exists'    "The table should now exist"
                Expect.isTrue alsoExists' "The key index should now exist"
            }
        ]
        testList "insert" [
            testTask "succeeds" {
                use! db = SqliteDb.BuildDb()
                let! before = Find.all<SubDocument> SqliteDb.TableName
                Expect.equal before [] "There should be no documents in the table"
        
                let testDoc = { emptyDoc with Id = "turkey"; Sub = Some { Foo = "gobble"; Bar = "gobble" } }
                do! insert SqliteDb.TableName testDoc
                let! after = Find.all<JsonDocument> SqliteDb.TableName
                Expect.equal after [ testDoc ] "There should have been one document inserted"
            }
            testTask "fails for duplicate key" {
                use! db = SqliteDb.BuildDb()
                do! insert SqliteDb.TableName { emptyDoc with Id = "test" }
                Expect.throws
                    (fun () ->
                        insert SqliteDb.TableName {emptyDoc with Id = "test" } |> Async.AwaitTask |> Async.RunSynchronously)
                    "An exception should have been raised for duplicate document ID insert"
            }
        ]
        testList "save" [
            testTask "succeeds when a document is inserted" {
                use! db = SqliteDb.BuildDb()
                let! before = Find.all<JsonDocument> SqliteDb.TableName
                Expect.equal before [] "There should be no documents in the table"
        
                let testDoc = { emptyDoc with Id = "test"; Sub = Some { Foo = "a"; Bar = "b" } }
                do! save SqliteDb.TableName testDoc
                let! after = Find.all<JsonDocument> SqliteDb.TableName
                Expect.equal after [ testDoc ] "There should have been one document inserted"
            }
            testTask "succeeds when a document is updated" {
                use! db = SqliteDb.BuildDb()
                let testDoc = { emptyDoc with Id = "test"; Sub = Some { Foo = "a"; Bar = "b" } }
                do! insert SqliteDb.TableName testDoc
        
                let! before = Find.byId<string, JsonDocument> SqliteDb.TableName "test"
                Expect.isSome before "There should have been a document returned"
                Expect.equal before.Value testDoc "The document is not correct"
        
                let upd8Doc = { testDoc with Sub = Some { Foo = "c"; Bar = "d" } }
                do! save SqliteDb.TableName upd8Doc
                let! after = Find.byId<string, JsonDocument> SqliteDb.TableName "test"
                Expect.isSome after "There should have been a document returned post-update"
                Expect.equal after.Value upd8Doc "The updated document is not correct"
            }
        ]
        testList "Count" [
            testTask "all succeeds" {
                use! db = SqliteDb.BuildDb()
                do! loadDocs ()
        
                let! theCount = Count.all SqliteDb.TableName
                Expect.equal theCount 5L "There should have been 5 matching documents"
            }
            testTask "byField succeeds" {
                use! db = SqliteDb.BuildDb()
                do! loadDocs ()
        
                let! theCount = Count.byField SqliteDb.TableName "Value" EQ "purple"
                Expect.equal theCount 2L "There should have been 2 matching documents"
            }
        ]
        testList "Exists" [
            testList "byId" [
                testTask "succeeds when a document exists" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
        
                    let! exists = Exists.byId SqliteDb.TableName "three"
                    Expect.isTrue exists "There should have been an existing document"
                }
                testTask "succeeds when a document does not exist" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
        
                    let! exists = Exists.byId SqliteDb.TableName "seven"
                    Expect.isFalse exists "There should not have been an existing document"
                }
            ]
            testList "byField" [
                testTask "succeeds when documents exist" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
        
                    let! exists = Exists.byField SqliteDb.TableName "NumValue" EQ 10
                    Expect.isTrue exists "There should have been existing documents"
                }
                testTask "succeeds when no matching documents exist" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
        
                    let! exists = Exists.byField SqliteDb.TableName "Nothing" LT "none"
                    Expect.isFalse exists "There should not have been any existing documents"
                }
            ]
        ]
        testList "Find" [
            testList "all" [
                testTask "succeeds when there is data" {
                    use! db = SqliteDb.BuildDb()
        
                    do! insert SqliteDb.TableName { Foo = "one"; Bar = "two" }
                    do! insert SqliteDb.TableName { Foo = "three"; Bar = "four" }
                    do! insert SqliteDb.TableName { Foo = "five"; Bar = "six" }
        
                    let! results = Find.all<SubDocument> SqliteDb.TableName
                    let expected = [
                        { Foo = "one"; Bar = "two" }
                        { Foo = "three"; Bar = "four" }
                        { Foo = "five"; Bar = "six" }
                    ]
                    Expect.equal results expected "There should have been 3 documents returned"
                }
                testTask "succeeds when there is no data" {
                    use! db = SqliteDb.BuildDb()
                    let! results = Find.all<SubDocument> SqliteDb.TableName
                    Expect.equal results [] "There should have been no documents returned"
                }
            ]
            testList "byId" [
                testTask "succeeds when a document is found" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
        
                    let! doc = Find.byId<string, JsonDocument> SqliteDb.TableName "two"
                    Expect.isTrue (Option.isSome doc) "There should have been a document returned"
                    Expect.equal doc.Value.Id "two" "The incorrect document was returned"
                }
                testTask "succeeds when a document is not found" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
        
                    let! doc = Find.byId<string, JsonDocument> SqliteDb.TableName "three hundred eighty-seven"
                    Expect.isFalse (Option.isSome doc) "There should not have been a document returned"
                }
            ]
            testList "byField" [
                testTask "succeeds when documents are found" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
        
                    let! docs = Find.byField<JsonDocument> SqliteDb.TableName "NumValue" GT 15
                    Expect.equal (List.length docs) 2 "There should have been two documents returned"
                }
                testTask "succeeds when documents are not found" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
        
                    let! docs = Find.byField<JsonDocument> SqliteDb.TableName "NumValue" GT 100
                    Expect.isTrue (List.isEmpty docs) "There should have been no documents returned"
                }
            ]
            testList "firstByField" [
                testTask "succeeds when a document is found" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
        
                    let! doc = Find.firstByField<JsonDocument> SqliteDb.TableName "Value" EQ "another"
                    Expect.isTrue (Option.isSome doc) "There should have been a document returned"
                    Expect.equal doc.Value.Id "two" "The incorrect document was returned"
                }
                testTask "succeeds when multiple documents are found" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
        
                    let! doc = Find.firstByField<JsonDocument> SqliteDb.TableName "Sub.Foo" EQ "green"
                    Expect.isTrue (Option.isSome doc) "There should have been a document returned"
                    Expect.contains [ "two"; "four" ] doc.Value.Id "An incorrect document was returned"
                }
                testTask "succeeds when a document is not found" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
        
                    let! doc = Find.firstByField<JsonDocument> SqliteDb.TableName "Value" EQ "absent"
                    Expect.isFalse (Option.isSome doc) "There should not have been a document returned"
                }
            ]
        ]
        testList "Update" [
            testList "full" [
                testTask "succeeds when a document is updated" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
        
                    let testDoc = { emptyDoc with Id = "one"; Sub = Some { Foo = "blue"; Bar = "red" } }
                    do! Update.full SqliteDb.TableName "one" testDoc
                    let! after = Find.byId<string, JsonDocument> SqliteDb.TableName "one"
                    Expect.isSome after "There should have been a document returned post-update"
                    Expect.equal after.Value testDoc "The updated document is not correct"
                }
                testTask "succeeds when no document is updated" {
                    use! db = SqliteDb.BuildDb()
        
                    let! before = Find.all<JsonDocument> SqliteDb.TableName
                    Expect.isEmpty before "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.full
                            SqliteDb.TableName
                            "test"
                            { emptyDoc with Id = "x"; Sub = Some { Foo = "blue"; Bar = "red" } }
                }
            ]
            testList "fullFunc" [
                testTask "succeeds when a document is updated" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
        
                    do! Update.fullFunc SqliteDb.TableName (_.Id) { Id = "one"; Value = "le un"; NumValue = 1; Sub = None }
                    let! after = Find.byId<string, JsonDocument> SqliteDb.TableName "one"
                    Expect.isSome after "There should have been a document returned post-update"
                    Expect.equal
                        after.Value
                        { Id = "one"; Value = "le un"; NumValue = 1; Sub = None }
                        "The updated document is not correct"
                }
                testTask "succeeds when no document is updated" {
                    use! db = SqliteDb.BuildDb()
        
                    let! before = Find.all<JsonDocument> SqliteDb.TableName
                    Expect.isEmpty before "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.fullFunc SqliteDb.TableName (_.Id) { Id = "one"; Value = "le un"; NumValue = 1; Sub = None }
                }
            ]
            testList "partialById" [
                testTask "succeeds when a document is updated" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
                    
                    do! Update.partialById SqliteDb.TableName "one" {| NumValue = 44 |}
                    let! after = Find.byId<string, JsonDocument> SqliteDb.TableName "one"
                    Expect.isSome after "There should have been a document returned post-update"
                    Expect.equal after.Value.NumValue 44 "The updated document is not correct"
                }
                testTask "succeeds when no document is updated" {
                    use! db = SqliteDb.BuildDb()
        
                    let! before = Find.all<SubDocument> SqliteDb.TableName
                    Expect.isEmpty before "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.partialById SqliteDb.TableName "test" {| Foo = "green" |}
                }
            ]
            testList "partialByField" [
                testTask "succeeds when a document is updated" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
                    
                    do! Update.partialByField SqliteDb.TableName "Value" EQ "purple" {| NumValue = 77 |}
                    let! after = Count.byField SqliteDb.TableName "NumValue" EQ 77
                    Expect.equal after 2L "There should have been 2 documents returned"
                }
                testTask "succeeds when no document is updated" {
                    use! db = SqliteDb.BuildDb()
        
                    let! before = Find.all<SubDocument> SqliteDb.TableName
                    Expect.isEmpty before "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.partialByField SqliteDb.TableName "Value" EQ "burgundy" {| Foo = "green" |}
                }
            ]
        ]
        testList "Delete" [
            testList "byId" [
                testTask "succeeds when a document is deleted" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
        
                    do! Delete.byId SqliteDb.TableName "four"
                    let! remaining = Count.all SqliteDb.TableName
                    Expect.equal remaining 4L "There should have been 4 documents remaining"
                }
                testTask "succeeds when a document is not deleted" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
        
                    do! Delete.byId SqliteDb.TableName "thirty"
                    let! remaining = Count.all SqliteDb.TableName
                    Expect.equal remaining 5L "There should have been 5 documents remaining"
                }
            ]
            testList "byField" [
                testTask "succeeds when documents are deleted" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
        
                    do! Delete.byField SqliteDb.TableName "Value" NE "purple"
                    let! remaining = Count.all SqliteDb.TableName
                    Expect.equal remaining 2L "There should have been 2 documents remaining"
                }
                testTask "succeeds when documents are not deleted" {
                    use! db = SqliteDb.BuildDb()
                    do! loadDocs ()
        
                    do! Delete.byField SqliteDb.TableName "Value" EQ "crimson"
                    let! remaining = Count.all SqliteDb.TableName
                    Expect.equal remaining 5L "There should have been 5 documents remaining"
                }
            ]
        ]
        test "clean up database" {
            Configuration.useConnectionString "data source=:memory:"
        }
    ]
    |> testSequenced

let all = testList "Sqlite" [ unitTests; integrationTests ]
