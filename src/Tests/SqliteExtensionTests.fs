module SqliteExtensionTests

open System.Text.Json
open BitBadger.Documents
open BitBadger.Documents.Sqlite
open BitBadger.Documents.Tests
open Expecto
open Microsoft.Data.Sqlite
open Types

/// Integration tests for the F# extensions on the SqliteConnection data type
let integrationTests =
    let loadDocs () = backgroundTask {
        for doc in testDocuments do do! insert SqliteDb.TableName doc
    }
    testList "Sqlite.Extensions" [
        testTask "ensureTable succeeds" {
            use! db   = SqliteDb.BuildDb()
            use  conn = Configuration.dbConn ()
            let itExists (name: string) =
                conn.customScalar
                    $"SELECT EXISTS (SELECT 1 FROM {SqliteDb.Catalog} WHERE name = @name) AS it"
                    [ SqliteParameter("@name", name) ]
                    toExists
            
            let! exists     = itExists "ensured"
            let! alsoExists = itExists "idx_ensured_key"
            Expect.isFalse exists     "The table should not exist already"
            Expect.isFalse alsoExists "The key index should not exist already"
    
            do! conn.ensureTable "ensured"
            let! exists'     = itExists "ensured"
            let! alsoExists' = itExists "idx_ensured_key"
            Expect.isTrue exists'    "The table should now exist"
            Expect.isTrue alsoExists' "The key index should now exist"
        }
        testTask "ensureFieldIndex succeeds" {
            use! db   = SqliteDb.BuildDb()
            use  conn = Configuration.dbConn ()
            let indexExists () =
                conn.customScalar
                    $"SELECT EXISTS (SELECT 1 FROM {SqliteDb.Catalog} WHERE name = 'idx_ensured_test') AS it"
                    []
                    toExists
            
            let! exists = indexExists ()
            Expect.isFalse exists "The index should not exist already"
    
            do! conn.ensureTable      "ensured"
            do! conn.ensureFieldIndex "ensured" "test" [ "Name"; "Age" ]
            let! exists' = indexExists ()
            Expect.isTrue exists' "The index should now exist"
        }
        testList "insert" [
            testTask "succeeds" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                let! before = conn.findAll<SubDocument> SqliteDb.TableName
                Expect.equal before [] "There should be no documents in the table"
        
                let testDoc = { emptyDoc with Id = "turkey"; Sub = Some { Foo = "gobble"; Bar = "gobble" } }
                do! conn.insert SqliteDb.TableName testDoc
                let! after = conn.findAll<JsonDocument> SqliteDb.TableName
                Expect.equal after [ testDoc ] "There should have been one document inserted"
            }
            testTask "fails for duplicate key" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! conn.insert SqliteDb.TableName { emptyDoc with Id = "test" }
                Expect.throws
                    (fun () ->
                        conn.insert SqliteDb.TableName {emptyDoc with Id = "test" }
                        |> Async.AwaitTask
                        |> Async.RunSynchronously)
                    "An exception should have been raised for duplicate document ID insert"
            }
        ]
        testList "save" [
            testTask "succeeds when a document is inserted" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                let! before = conn.findAll<JsonDocument> SqliteDb.TableName
                Expect.equal before [] "There should be no documents in the table"
        
                let testDoc = { emptyDoc with Id = "test"; Sub = Some { Foo = "a"; Bar = "b" } }
                do! conn.save SqliteDb.TableName testDoc
                let! after = conn.findAll<JsonDocument> SqliteDb.TableName
                Expect.equal after [ testDoc ] "There should have been one document inserted"
            }
            testTask "succeeds when a document is updated" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                let testDoc = { emptyDoc with Id = "test"; Sub = Some { Foo = "a"; Bar = "b" } }
                do! conn.insert SqliteDb.TableName testDoc
        
                let! before = conn.findById<string, JsonDocument> SqliteDb.TableName "test"
                if Option.isNone before then Expect.isTrue false "There should have been a document returned"
                Expect.equal before.Value testDoc "The document is not correct"
        
                let upd8Doc = { testDoc with Sub = Some { Foo = "c"; Bar = "d" } }
                do! conn.save SqliteDb.TableName upd8Doc
                let! after = conn.findById<string, JsonDocument> SqliteDb.TableName "test"
                if Option.isNone after then
                    Expect.isTrue false "There should have been a document returned post-update"
                Expect.equal after.Value upd8Doc "The updated document is not correct"
            }
        ]
        testTask "countAll succeeds" {
            use! db   = SqliteDb.BuildDb()
            use  conn = Configuration.dbConn ()
            do! loadDocs ()
    
            let! theCount = conn.countAll SqliteDb.TableName
            Expect.equal theCount 5L "There should have been 5 matching documents"
        }
        testTask "countByField succeeds" {
            use! db   = SqliteDb.BuildDb()
            use  conn = Configuration.dbConn ()
            do! loadDocs ()
    
            let! theCount = conn.countByField SqliteDb.TableName (Field.EQ "Value" "purple")
            Expect.equal theCount 2L "There should have been 2 matching documents"
        }
        testList "existsById" [
            testTask "succeeds when a document exists" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                let! exists = conn.existsById SqliteDb.TableName "three"
                Expect.isTrue exists "There should have been an existing document"
            }
            testTask "succeeds when a document does not exist" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                let! exists = conn.existsById SqliteDb.TableName "seven"
                Expect.isFalse exists "There should not have been an existing document"
            }
        ]
        testList "existsByField" [
            testTask "succeeds when documents exist" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                let! exists = conn.existsByField SqliteDb.TableName (Field.EQ "NumValue" 10)
                Expect.isTrue exists "There should have been existing documents"
            }
            testTask "succeeds when no matching documents exist" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                let! exists = conn.existsByField SqliteDb.TableName (Field.EQ "Nothing" "none")
                Expect.isFalse exists "There should not have been any existing documents"
            }
        ]
        testList "findAll" [
            testTask "succeeds when there is data" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
    
                do! insert SqliteDb.TableName { Foo = "one"; Bar = "two" }
                do! insert SqliteDb.TableName { Foo = "three"; Bar = "four" }
                do! insert SqliteDb.TableName { Foo = "five"; Bar = "six" }
    
                let! results = conn.findAll<SubDocument> SqliteDb.TableName
                let expected = [
                    { Foo = "one"; Bar = "two" }
                    { Foo = "three"; Bar = "four" }
                    { Foo = "five"; Bar = "six" }
                ]
                Expect.equal results expected "There should have been 3 documents returned"
            }
            testTask "succeeds when there is no data" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                let! results = conn.findAll<SubDocument> SqliteDb.TableName
                Expect.equal results [] "There should have been no documents returned"
            }
        ]
        testList "findById" [
            testTask "succeeds when a document is found" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                let! doc = conn.findById<string, JsonDocument> SqliteDb.TableName "two"
                Expect.isTrue (Option.isSome doc) "There should have been a document returned"
                Expect.equal doc.Value.Id "two" "The incorrect document was returned"
            }
            testTask "succeeds when a document is not found" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                let! doc = conn.findById<string, JsonDocument> SqliteDb.TableName "three hundred eighty-seven"
                Expect.isFalse (Option.isSome doc) "There should not have been a document returned"
            }
        ]
        testList "findByField" [
            testTask "succeeds when documents are found" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                let! docs = conn.findByField<JsonDocument> SqliteDb.TableName (Field.EQ "Sub.Foo" "green")
                Expect.equal (List.length docs) 2 "There should have been two documents returned"
            }
            testTask "succeeds when documents are not found" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                let! docs = conn.findByField<JsonDocument> SqliteDb.TableName (Field.EQ "Value" "mauve")
                Expect.isTrue (List.isEmpty docs) "There should have been no documents returned"
            }
        ]
        testList "findFirstByField" [
            testTask "succeeds when a document is found" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                let! doc = conn.findFirstByField<JsonDocument> SqliteDb.TableName (Field.EQ "Value" "another")
                Expect.isTrue (Option.isSome doc) "There should have been a document returned"
                Expect.equal doc.Value.Id "two" "The incorrect document was returned"
            }
            testTask "succeeds when multiple documents are found" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                let! doc = conn.findFirstByField<JsonDocument> SqliteDb.TableName (Field.EQ "Sub.Foo" "green")
                Expect.isTrue (Option.isSome doc) "There should have been a document returned"
                Expect.contains [ "two"; "four" ] doc.Value.Id "An incorrect document was returned"
            }
            testTask "succeeds when a document is not found" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                let! doc = conn.findFirstByField<JsonDocument> SqliteDb.TableName (Field.EQ "Value" "absent")
                Expect.isFalse (Option.isSome doc) "There should not have been a document returned"
            }
        ]
        testList "updateById" [
            testTask "succeeds when a document is updated" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                let testDoc = { emptyDoc with Id = "one"; Sub = Some { Foo = "blue"; Bar = "red" } }
                do! conn.updateById SqliteDb.TableName "one" testDoc
                let! after = conn.findById<string, JsonDocument> SqliteDb.TableName "one"
                if Option.isNone after then
                    Expect.isTrue false "There should have been a document returned post-update"
                Expect.equal after.Value testDoc "The updated document is not correct"
            }
            testTask "succeeds when no document is updated" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
    
                let! before = conn.findAll<JsonDocument> SqliteDb.TableName
                Expect.isEmpty before "There should have been no documents returned"
                
                // This not raising an exception is the test
                do! conn.updateById
                        SqliteDb.TableName
                        "test"
                        { emptyDoc with Id = "x"; Sub = Some { Foo = "blue"; Bar = "red" } }
            }
        ]
        testList "updateByFunc" [
            testTask "succeeds when a document is updated" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                do! conn.updateByFunc
                        SqliteDb.TableName (_.Id) { Id = "one"; Value = "le un"; NumValue = 1; Sub = None }
                let! after = conn.findById<string, JsonDocument> SqliteDb.TableName "one"
                if Option.isNone after then
                    Expect.isTrue false "There should have been a document returned post-update"
                Expect.equal
                    after.Value
                    { Id = "one"; Value = "le un"; NumValue = 1; Sub = None }
                    "The updated document is not correct"
            }
            testTask "succeeds when no document is updated" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
    
                let! before = conn.findAll<JsonDocument> SqliteDb.TableName
                Expect.isEmpty before "There should have been no documents returned"
                
                // This not raising an exception is the test
                do! conn.updateByFunc
                        SqliteDb.TableName (_.Id) { Id = "one"; Value = "le un"; NumValue = 1; Sub = None }
            }
        ]
        testList "patchById" [
            testTask "succeeds when a document is updated" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
                
                do! conn.patchById SqliteDb.TableName "one" {| NumValue = 44 |}
                let! after = conn.findById<string, JsonDocument> SqliteDb.TableName "one"
                if Option.isNone after then
                    Expect.isTrue false "There should have been a document returned post-update"
                Expect.equal after.Value.NumValue 44 "The updated document is not correct"
            }
            testTask "succeeds when no document is updated" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
    
                let! before = conn.findAll<SubDocument> SqliteDb.TableName
                Expect.isEmpty before "There should have been no documents returned"
                
                // This not raising an exception is the test
                do! conn.patchById SqliteDb.TableName "test" {| Foo = "green" |}
            }
        ]
        testList "patchByField" [
            testTask "succeeds when a document is updated" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
                
                do! conn.patchByField SqliteDb.TableName (Field.EQ "Value" "purple") {| NumValue = 77 |}
                let! after = conn.countByField SqliteDb.TableName (Field.EQ "NumValue" 77)
                Expect.equal after 2L "There should have been 2 documents returned"
            }
            testTask "succeeds when no document is updated" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
    
                let! before = conn.findAll<SubDocument> SqliteDb.TableName
                Expect.isEmpty before "There should have been no documents returned"
                
                // This not raising an exception is the test
                do! conn.patchByField SqliteDb.TableName (Field.EQ "Value" "burgundy") {| Foo = "green" |}
            }
        ]
        testList "removeFieldById" [
            testTask "succeeds when a field is removed" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
                
                do! conn.removeFieldById SqliteDb.TableName "two" "Sub"
                try
                    let! _ = conn.findById<string, JsonDocument> SqliteDb.TableName "two"
                    Expect.isTrue false "The updated document should have failed to parse"
                with
                | :? JsonException -> ()
                | exn as ex -> Expect.isTrue false $"Threw {ex.GetType().Name} ({ex.Message})"
            }
            testTask "succeeds when a field is not removed" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
                
                // This not raising an exception is the test
                do! conn.removeFieldById SqliteDb.TableName "two" "AFieldThatIsNotThere"
            }
            testTask "succeeds when no document is matched" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                
                // This not raising an exception is the test
                do! conn.removeFieldById SqliteDb.TableName "two" "Value"
            }
        ]
        testList "removeFieldByField" [
            testTask "succeeds when a field is removed" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
                
                do! conn.removeFieldByField SqliteDb.TableName (Field.EQ "NumValue" 17) "Sub"
                try
                    let! _ = conn.findById<string, JsonDocument> SqliteDb.TableName "four"
                    Expect.isTrue false "The updated document should have failed to parse"
                with
                | :? JsonException -> ()
                | exn as ex -> Expect.isTrue false $"Threw {ex.GetType().Name} ({ex.Message})"
            }
            testTask "succeeds when a field is not removed" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
                
                // This not raising an exception is the test
                do! conn.removeFieldByField SqliteDb.TableName (Field.EQ "NumValue" 17) "Nothing"
            }
            testTask "succeeds when no document is matched" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                
                // This not raising an exception is the test
                do! conn.removeFieldByField SqliteDb.TableName (Field.NE "Abracadabra" "apple") "Value"
            }
        ]
        testList "deleteById" [
            testTask "succeeds when a document is deleted" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                do! conn.deleteById SqliteDb.TableName "four"
                let! remaining = conn.countAll SqliteDb.TableName
                Expect.equal remaining 4L "There should have been 4 documents remaining"
            }
            testTask "succeeds when a document is not deleted" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                do! conn.deleteById SqliteDb.TableName "thirty"
                let! remaining = conn.countAll SqliteDb.TableName
                Expect.equal remaining 5L "There should have been 5 documents remaining"
            }
        ]
        testList "deleteByField" [
            testTask "succeeds when documents are deleted" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                do! conn.deleteByField SqliteDb.TableName (Field.NE "Value" "purple")
                let! remaining = conn.countAll SqliteDb.TableName
                Expect.equal remaining 2L "There should have been 2 documents remaining"
            }
            testTask "succeeds when documents are not deleted" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                do! conn.deleteByField SqliteDb.TableName (Field.EQ "Value" "crimson")
                let! remaining = conn.countAll SqliteDb.TableName
                Expect.equal remaining 5L "There should have been 5 documents remaining"
            }
        ]
        testList "customSingle" [
            testTask "succeeds when a row is found" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                let! doc =
                    conn.customSingle
                        $"SELECT data FROM {SqliteDb.TableName} WHERE data ->> 'Id' = @id"
                        [ SqliteParameter("@id", "one") ]
                        fromData<JsonDocument>
                Expect.isSome doc "There should have been a document returned"
                Expect.equal doc.Value.Id "one" "The incorrect document was returned"
            }
            testTask "succeeds when a row is not found" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                let! doc =
                    conn.customSingle
                        $"SELECT data FROM {SqliteDb.TableName} WHERE data ->> 'Id' = @id"
                        [ SqliteParameter("@id", "eighty") ]
                        fromData<JsonDocument>
                Expect.isNone doc "There should not have been a document returned"
            }
        ]
        testList "customList" [
            testTask "succeeds when data is found" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                let! docs = conn.customList (Query.selectFromTable SqliteDb.TableName) [] fromData<JsonDocument>
                Expect.hasCountOf docs 5u (fun _ -> true) "There should have been 5 documents returned"
            }
            testTask "succeeds when data is not found" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                let! docs =
                    conn.customList
                        $"SELECT data FROM {SqliteDb.TableName} WHERE data ->> 'NumValue' > @value"
                        [ SqliteParameter("@value", 100) ]
                        fromData<JsonDocument>
                Expect.isEmpty docs "There should have been no documents returned"
            }
        ]
        testList "customNonQuery" [
            testTask "succeeds when operating on data" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()

                do! conn.customNonQuery $"DELETE FROM {SqliteDb.TableName}" []

                let! remaining = conn.countAll SqliteDb.TableName
                Expect.equal remaining 0L "There should be no documents remaining in the table"
            }
            testTask "succeeds when no data matches where clause" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()

                do! conn.customNonQuery
                        $"DELETE FROM {SqliteDb.TableName} WHERE data ->> 'NumValue' > @value"
                        [ SqliteParameter("@value", 100) ]

                let! remaining = conn.countAll SqliteDb.TableName
                Expect.equal remaining 5L "There should be 5 documents remaining in the table"
            }
        ]
        testTask "customScalar succeeds" {
            use! db   = SqliteDb.BuildDb()
            use  conn = Configuration.dbConn ()
    
            let! nbr = conn.customScalar "SELECT 5 AS test_value" [] _.GetInt32(0)
            Expect.equal nbr 5 "The query should have returned the number 5"
        }
        test "clean up database" {
            Configuration.useConnectionString "data source=:memory:"
        }
    ]
    |> testSequenced
