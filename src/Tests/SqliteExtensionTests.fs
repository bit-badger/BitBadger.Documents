module SqliteExtensionTests

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
            let itExists (name: string) = task {
                let! result =
                    conn.customScalar
                        $"SELECT EXISTS (SELECT 1 FROM {SqliteDb.Catalog} WHERE name = @name) AS it"
                        [ SqliteParameter("@name", name) ]
                        _.GetInt64(0)
                return result > 0
            }
            
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
    
            let! theCount = conn.countByField SqliteDb.TableName "Value" EQ "purple"
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
    
                let! exists = conn.existsByField SqliteDb.TableName "NumValue" EQ 10
                Expect.isTrue exists "There should have been existing documents"
            }
            testTask "succeeds when no matching documents exist" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                let! exists = conn.existsByField SqliteDb.TableName "Nothing" EQ "none"
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
    
                let! docs = conn.findByField<JsonDocument> SqliteDb.TableName "Sub.Foo" EQ "green"
                Expect.equal (List.length docs) 2 "There should have been two documents returned"
            }
            testTask "succeeds when documents are not found" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                let! docs = conn.findByField<JsonDocument> SqliteDb.TableName "Value" EQ "mauve"
                Expect.isTrue (List.isEmpty docs) "There should have been no documents returned"
            }
        ]
        testList "findFirstByField" [
            testTask "succeeds when a document is found" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                let! doc = conn.findFirstByField<JsonDocument> SqliteDb.TableName "Value" EQ "another"
                Expect.isTrue (Option.isSome doc) "There should have been a document returned"
                Expect.equal doc.Value.Id "two" "The incorrect document was returned"
            }
            testTask "succeeds when multiple documents are found" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                let! doc = conn.findFirstByField<JsonDocument> SqliteDb.TableName "Sub.Foo" EQ "green"
                Expect.isTrue (Option.isSome doc) "There should have been a document returned"
                Expect.contains [ "two"; "four" ] doc.Value.Id "An incorrect document was returned"
            }
            testTask "succeeds when a document is not found" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                let! doc = conn.findFirstByField<JsonDocument> SqliteDb.TableName "Value" EQ "absent"
                Expect.isFalse (Option.isSome doc) "There should not have been a document returned"
            }
        ]
        testList "updateFull" [
            testTask "succeeds when a document is updated" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                let testDoc = { emptyDoc with Id = "one"; Sub = Some { Foo = "blue"; Bar = "red" } }
                do! conn.updateFull SqliteDb.TableName "one" testDoc
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
                do! conn.updateFull
                        SqliteDb.TableName
                        "test"
                        { emptyDoc with Id = "x"; Sub = Some { Foo = "blue"; Bar = "red" } }
            }
        ]
        testList "updateFullFunc" [
            testTask "succeeds when a document is updated" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                do! conn.updateFullFunc
                        SqliteDb.TableName
                        (_.Id)
                        { Id = "one"; Value = "le un"; NumValue = 1; Sub = None }
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
                do! conn.updateFullFunc
                        SqliteDb.TableName
                        (_.Id)
                        { Id = "one"; Value = "le un"; NumValue = 1; Sub = None }
            }
        ]
        testList "updatePartialById" [
            testTask "succeeds when a document is updated" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
                
                do! conn.updatePartialById SqliteDb.TableName "one" {| NumValue = 44 |}
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
                do! conn.updatePartialById SqliteDb.TableName "test" {| Foo = "green" |}
            }
        ]
        testList "updatePartialByField" [
            testTask "succeeds when a document is updated" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
                
                do! conn.updatePartialByField SqliteDb.TableName "Value" EQ "purple" {| NumValue = 77 |}
                let! after = conn.countByField SqliteDb.TableName "NumValue" EQ 77
                Expect.equal after 2L "There should have been 2 documents returned"
            }
            testTask "succeeds when no document is updated" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
    
                let! before = conn.findAll<SubDocument> SqliteDb.TableName
                Expect.isEmpty before "There should have been no documents returned"
                
                // This not raising an exception is the test
                do! conn.updatePartialByField SqliteDb.TableName "Value" EQ "burgundy" {| Foo = "green" |}
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
    
                do! conn.deleteByField SqliteDb.TableName "Value" NE "purple"
                let! remaining = conn.countAll SqliteDb.TableName
                Expect.equal remaining 2L "There should have been 2 documents remaining"
            }
            testTask "succeeds when documents are not deleted" {
                use! db   = SqliteDb.BuildDb()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
    
                do! conn.deleteByField SqliteDb.TableName "Value" EQ "crimson"
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
