module PostgresExtensionTests

open BitBadger.Documents
open BitBadger.Documents.Postgres
open BitBadger.Documents.Tests
open Expecto
open Npgsql
open Types

/// Open a connection to the throwaway database
let private mkConn (db: ThrowawayPostgresDb) =
    let conn = new NpgsqlConnection(db.ConnectionString)
    conn.Open()
    conn

/// Integration tests for the F# extensions on the NpgsqlConnection data type
let integrationTests =
    let loadDocs (conn: NpgsqlConnection) = backgroundTask {
        for doc in testDocuments do do! conn.insert PostgresDb.TableName doc
    }
    testList "Postgres.Extensions" [
        testList "customList" [
            testTask "succeeds when data is found" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! docs = conn.customList (Query.selectFromTable PostgresDb.TableName) [] fromData<JsonDocument>
                Expect.equal (List.length docs) 5 "There should have been 5 documents returned"
            }
            testTask "succeeds when data is not found" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! docs =
                    conn.customList
                        $"SELECT data FROM {PostgresDb.TableName} WHERE data @? @path::jsonpath"
                        [ "@path", Sql.string "$.NumValue ? (@ > 100)" ]
                        fromData<JsonDocument>
                Expect.isEmpty docs "There should have been no documents returned"
            }
        ]
        testList "customSingle" [
            testTask "succeeds when a row is found" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! doc =
                    conn.customSingle
                        $"SELECT data FROM {PostgresDb.TableName} WHERE data ->> 'Id' = @id"
                        [ "@id", Sql.string "one"]
                        fromData<JsonDocument>
                Expect.isSome doc "There should have been a document returned"
                Expect.equal doc.Value.Id "one" "The incorrect document was returned"
            }
            testTask "succeeds when a row is not found" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! doc =
                    conn.customSingle
                        $"SELECT data FROM {PostgresDb.TableName} WHERE data ->> 'Id' = @id"
                        [ "@id", Sql.string "eighty" ]
                        fromData<JsonDocument>
                Expect.isNone doc "There should not have been a document returned"
            }
        ]
        testList "customNonQuery" [
            testTask "succeeds when operating on data" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                do! conn.customNonQuery $"DELETE FROM {PostgresDb.TableName}" []

                let! remaining = conn.countAll PostgresDb.TableName
                Expect.equal remaining 0 "There should be no documents remaining in the table"
            }
            testTask "succeeds when no data matches where clause" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                do! conn.customNonQuery
                        $"DELETE FROM {PostgresDb.TableName} WHERE data @? @path::jsonpath"
                        [ "@path", Sql.string "$.NumValue ? (@ > 100)" ]

                let! remaining = conn.countAll PostgresDb.TableName
                Expect.equal remaining 5 "There should be 5 documents remaining in the table"
            }
        ]
        testTask "scalar succeeds" {
            use  db   = PostgresDb.BuildDb()
            use  conn = mkConn db
            let! nbr  = conn.customScalar "SELECT 5 AS test_value" [] (fun row -> row.int "test_value")
            Expect.equal nbr 5 "The query should have returned the number 5"
        }
        testTask "ensureTable succeeds" {
            use db   = PostgresDb.BuildDb()
            use conn = mkConn db
            let tableExists () =
                conn.customScalar "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'ensured') AS it" [] toExists
            let keyExists () =
                conn.customScalar
                    "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'idx_ensured_key') AS it" [] toExists
            
            let! exists     = tableExists ()
            let! alsoExists = keyExists   ()
            Expect.isFalse exists     "The table should not exist already"
            Expect.isFalse alsoExists "The key index should not exist already"

            do! conn.ensureTable "ensured"
            let! exists'     = tableExists ()
            let! alsoExists' = keyExists   ()
            Expect.isTrue exists'    "The table should now exist"
            Expect.isTrue alsoExists' "The key index should now exist"
        }
        testTask "ensureDocumentIndex succeeds" {
            use db   = PostgresDb.BuildDb()
            use conn = mkConn db
            let indexExists () =
                conn.customScalar
                    "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'idx_ensured_document') AS it" [] toExists
            
            let! exists = indexExists ()
            Expect.isFalse exists "The index should not exist already"

            do! conn.ensureTable         "ensured"
            do! conn.ensureDocumentIndex "ensured" Optimized
            let! exists' = indexExists ()
            Expect.isTrue exists' "The index should now exist"
        }
        testTask "ensureFieldIndex succeeds" {
            use db   = PostgresDb.BuildDb()
            use conn = mkConn db
            let indexExists () =
                conn.customScalar
                    "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'idx_ensured_test') AS it" [] toExists
            
            let! exists = indexExists ()
            Expect.isFalse exists "The index should not exist already"

            do! conn.ensureTable      "ensured"
            do! conn.ensureFieldIndex "ensured" "test" [ "Id"; "Category" ]
            let! exists' = indexExists ()
            Expect.isTrue exists' "The index should now exist"
        }
        testList "insert" [
            testTask "succeeds" {
                use  db     = PostgresDb.BuildDb()
                use  conn   = mkConn db
                let! before = conn.countAll PostgresDb.TableName
                Expect.equal before 0 "There should be no documents in the table"

                let testDoc = { emptyDoc with Id = "turkey"; Sub = Some { Foo = "gobble"; Bar = "gobble" } }
                do! conn.insert PostgresDb.TableName testDoc
                let! after = conn.findAll<JsonDocument> PostgresDb.TableName
                Expect.equal after [ testDoc ] "There should have been one document inserted"
            }
            testTask "fails for duplicate key" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! conn.insert PostgresDb.TableName { emptyDoc with Id = "test" }
                Expect.throws
                    (fun () ->
                        conn.insert PostgresDb.TableName {emptyDoc with Id = "test" }
                        |> Async.AwaitTask
                        |> Async.RunSynchronously)
                    "An exception should have been raised for duplicate document ID insert"
            }
        ]
        testList "save" [
            testTask "succeeds when a document is inserted" {
                use  db     = PostgresDb.BuildDb()
                use  conn   = mkConn db
                let! before = conn.countAll PostgresDb.TableName
                Expect.equal before 0 "There should be no documents in the table"

                let testDoc = { emptyDoc with Id = "test"; Sub = Some { Foo = "a"; Bar = "b" } }
                do! conn.save PostgresDb.TableName testDoc
                let! after = conn.findAll<JsonDocument> PostgresDb.TableName
                Expect.equal after [ testDoc ] "There should have been one document inserted"
            }
            testTask "succeeds when a document is updated" {
                use db      = PostgresDb.BuildDb()
                use conn    = mkConn db
                let testDoc = { emptyDoc with Id = "test"; Sub = Some { Foo = "a"; Bar = "b" } }
                do! conn.insert PostgresDb.TableName testDoc

                let! before = conn.findById<string, JsonDocument> PostgresDb.TableName "test"
                Expect.isSome before "There should have been a document returned"
                Expect.equal before.Value testDoc "The document is not correct"

                let upd8Doc = { testDoc with Sub = Some { Foo = "c"; Bar = "d" } }
                do! conn.save PostgresDb.TableName upd8Doc
                let! after = conn.findById<string, JsonDocument> PostgresDb.TableName "test"
                Expect.isSome after "There should have been a document returned post-update"
                Expect.equal after.Value upd8Doc "The updated document is not correct"
            }
        ]
        testTask "countAll succeeds" {
            use db   = PostgresDb.BuildDb()
            use conn = mkConn db
            do! loadDocs conn

            let! theCount = conn.countAll PostgresDb.TableName
            Expect.equal theCount 5 "There should have been 5 matching documents"
        }
        testTask "countByField succeeds" {
            use db   = PostgresDb.BuildDb()
            use conn = mkConn db
            do! loadDocs conn
            
            let! theCount = conn.countByField PostgresDb.TableName "Value" EQ "purple"
            Expect.equal theCount 2 "There should have been 2 matching documents"
        }
        testTask "countByContains succeeds" {
            use db   = PostgresDb.BuildDb()
            use conn = mkConn db
            do! loadDocs conn

            let! theCount = conn.countByContains PostgresDb.TableName {| Value = "purple" |}
            Expect.equal theCount 2 "There should have been 2 matching documents"
        }
        testTask "countByJsonPath succeeds" {
            use db   = PostgresDb.BuildDb()
            use conn = mkConn db
            do! loadDocs conn

            let! theCount = conn.countByJsonPath PostgresDb.TableName "$.NumValue ? (@ > 5)"
            Expect.equal theCount 3 "There should have been 3 matching documents"
        }
        testList "existsById" [
            testTask "succeeds when a document exists" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! exists = conn.existsById PostgresDb.TableName "three"
                Expect.isTrue exists "There should have been an existing document"
            }
            testTask "succeeds when a document does not exist" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! exists = conn.existsById PostgresDb.TableName "seven"
                Expect.isFalse exists "There should not have been an existing document"
            }
        ]
        testList "existsByField" [
            testTask "succeeds when documents exist" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! exists = conn.existsByField PostgresDb.TableName "Sub" EX ""
                Expect.isTrue exists "There should have been existing documents"
            }
            testTask "succeeds when documents do not exist" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! exists = conn.existsByField PostgresDb.TableName "NumValue" EQ "six"
                Expect.isFalse exists "There should not have been existing documents"
            }
        ]
        testList "existsByContains" [
            testTask "succeeds when documents exist" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! exists = conn.existsByContains PostgresDb.TableName {| NumValue = 10 |}
                Expect.isTrue exists "There should have been existing documents"
            }
            testTask "succeeds when no matching documents exist" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! exists = conn.existsByContains PostgresDb.TableName {| Nothing = "none" |}
                Expect.isFalse exists "There should not have been any existing documents"
            }
        ]
        testList "existsByJsonPath" [
            testTask "succeeds when documents exist" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! exists = conn.existsByJsonPath PostgresDb.TableName """$.Sub.Foo ? (@ == "green")"""
                Expect.isTrue exists "There should have been existing documents"
            }
            testTask "succeeds when no matching documents exist" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! exists = conn.existsByJsonPath PostgresDb.TableName "$.NumValue ? (@ > 1000)"
                Expect.isFalse exists "There should not have been any existing documents"
            }
        ]
        testList "findAll" [
            testTask "succeeds when there is data" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db

                do! conn.insert PostgresDb.TableName { Foo = "one"; Bar = "two" }
                do! conn.insert PostgresDb.TableName { Foo = "three"; Bar = "four" }
                do! conn.insert PostgresDb.TableName { Foo = "five"; Bar = "six" }

                let! results = conn.findAll<SubDocument> PostgresDb.TableName
                let expected = [
                    { Foo = "one"; Bar = "two" }
                    { Foo = "three"; Bar = "four" }
                    { Foo = "five"; Bar = "six" }
                ]
                Expect.equal results expected "There should have been 3 documents returned"
            }
            testTask "succeeds when there is no data" {
                use  db      = PostgresDb.BuildDb()
                use  conn    = mkConn db
                let! results = conn.findAll<SubDocument> PostgresDb.TableName
                Expect.equal results [] "There should have been no documents returned"
            }
        ]
        testList "findById" [
            testTask "succeeds when a document is found" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! doc = conn.findById<string, JsonDocument> PostgresDb.TableName "two"
                Expect.isSome doc "There should have been a document returned"
                Expect.equal doc.Value.Id "two" "The incorrect document was returned"
            }
            testTask "succeeds when a document is not found" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! doc = conn.findById<string, JsonDocument> PostgresDb.TableName "three hundred eighty-seven"
                Expect.isNone doc "There should not have been a document returned"
            }
        ]
        testList "findByField" [
            testTask "succeeds when documents are found" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! docs = conn.findByField<JsonDocument> PostgresDb.TableName "Value" EQ "another"
                Expect.equal (List.length docs) 1 "There should have been one document returned"
            }
            testTask "succeeds when documents are not found" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! docs = conn.findByField<JsonDocument> PostgresDb.TableName "Value" EQ "mauve"
                Expect.isEmpty docs "There should have been no documents returned"
            }
        ]
        testList "findByContains" [
            testTask "succeeds when documents are found" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! docs = conn.findByContains<JsonDocument> PostgresDb.TableName {| Sub = {| Foo = "green" |} |}
                Expect.equal (List.length docs) 2 "There should have been two documents returned"
            }
            testTask "succeeds when documents are not found" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! docs = conn.findByContains<JsonDocument> PostgresDb.TableName {| Value = "mauve" |}
                Expect.isEmpty docs "There should have been no documents returned"
            }
        ]
        testList "findByJsonPath" [
            testTask "succeeds when documents are found" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! docs = conn.findByJsonPath<JsonDocument> PostgresDb.TableName "$.NumValue ? (@ < 15)"
                Expect.equal (List.length docs) 3 "There should have been 3 documents returned"
            }
            testTask "succeeds when documents are not found" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! docs = conn.findByJsonPath<JsonDocument> PostgresDb.TableName "$.NumValue ? (@ < 0)"
                Expect.isEmpty docs "There should have been no documents returned"
            }
        ]
        testList "findFirstByField" [
            testTask "succeeds when a document is found" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! doc = conn.findFirstByField<JsonDocument> PostgresDb.TableName "Value" EQ "another"
                Expect.isSome doc "There should have been a document returned"
                Expect.equal doc.Value.Id "two" "The incorrect document was returned"
            }
            testTask "succeeds when multiple documents are found" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! doc = conn.findFirstByField<JsonDocument> PostgresDb.TableName "Value" EQ "purple"
                Expect.isSome doc "There should have been a document returned"
                Expect.contains [ "five"; "four" ] doc.Value.Id "An incorrect document was returned"
            }
            testTask "succeeds when a document is not found" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! doc = conn.findFirstByField<JsonDocument> PostgresDb.TableName "Value" EQ "absent"
                Expect.isNone doc "There should not have been a document returned"
            }
        ]
        testList "findFirstByContains" [
            testTask "succeeds when a document is found" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! doc = conn.findFirstByContains<JsonDocument> PostgresDb.TableName {| Value = "another" |}
                Expect.isSome doc "There should have been a document returned"
                Expect.equal doc.Value.Id "two" "The incorrect document was returned"
            }
            testTask "succeeds when multiple documents are found" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! doc = conn.findFirstByContains<JsonDocument> PostgresDb.TableName {| Sub = {| Foo = "green" |} |}
                Expect.isSome doc "There should have been a document returned"
                Expect.contains [ "two"; "four" ] doc.Value.Id "An incorrect document was returned"
            }
            testTask "succeeds when a document is not found" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! doc = conn.findFirstByContains<JsonDocument> PostgresDb.TableName {| Value = "absent" |}
                Expect.isNone doc "There should not have been a document returned"
            }
        ]
        testList "findFirstByJsonPath" [
            testTask "succeeds when a document is found" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! doc = conn.findFirstByJsonPath<JsonDocument> PostgresDb.TableName """$.Value ? (@ == "FIRST!")"""
                Expect.isSome doc "There should have been a document returned"
                Expect.equal doc.Value.Id "one" "The incorrect document was returned"
            }
            testTask "succeeds when multiple documents are found" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! doc = conn.findFirstByJsonPath<JsonDocument> PostgresDb.TableName """$.Sub.Foo ? (@ == "green")"""
                Expect.isSome doc "There should have been a document returned"
                Expect.contains [ "two"; "four" ] doc.Value.Id "An incorrect document was returned"
            }
            testTask "succeeds when a document is not found" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let! doc = conn.findFirstByJsonPath<JsonDocument> PostgresDb.TableName """$.Id ? (@ == "nope")"""
                Expect.isNone doc "There should not have been a document returned"
            }
        ]
        testList "updateFull" [
            testTask "succeeds when a document is updated" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                let testDoc = { emptyDoc with Id = "one"; Sub = Some { Foo = "blue"; Bar = "red" } }
                do! conn.updateFull PostgresDb.TableName "one" testDoc
                let! after = conn.findById<string, JsonDocument> PostgresDb.TableName "one"
                Expect.isSome after "There should have been a document returned post-update"
                Expect.equal after.Value testDoc "The updated document is not correct"
            }
            testTask "succeeds when no document is updated" {
                use  db     = PostgresDb.BuildDb()
                use  conn   = mkConn db
                let! before = conn.countAll PostgresDb.TableName
                Expect.equal before 0 "There should have been no documents returned"
                
                // This not raising an exception is the test
                do! conn.updateFull
                        PostgresDb.TableName "test" { emptyDoc with Id = "x"; Sub = Some { Foo = "blue"; Bar = "red" } }
            }
        ]
        testList "updateFullFunc" [
            testTask "succeeds when a document is updated" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                do! conn.updateFullFunc
                        PostgresDb.TableName (_.Id) { Id = "one"; Value = "le un"; NumValue = 1; Sub = None }
                let! after = conn.findById<string, JsonDocument> PostgresDb.TableName "one"
                Expect.isSome after "There should have been a document returned post-update"
                Expect.equal
                    after.Value
                    { Id = "one"; Value = "le un"; NumValue = 1; Sub = None }
                    "The updated document is not correct"
            }
            testTask "succeeds when no document is updated" {
                use  db     = PostgresDb.BuildDb()
                use  conn   = mkConn db
                let! before = conn.countAll PostgresDb.TableName
                Expect.equal before 0 "There should have been no documents returned"
                
                // This not raising an exception is the test
                do! conn.updateFullFunc
                        PostgresDb.TableName (_.Id) { Id = "one"; Value = "le un"; NumValue = 1; Sub = None }
            }
        ]
        testList "updatePartialById" [
            testTask "succeeds when a document is updated" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn
                
                do! conn.updatePartialById PostgresDb.TableName "one" {| NumValue = 44 |}
                let! after = conn.findById<string, JsonDocument> PostgresDb.TableName "one"
                Expect.isSome after "There should have been a document returned post-update"
                Expect.equal after.Value.NumValue 44 "The updated document is not correct"
            }
            testTask "succeeds when no document is updated" {
                use  db     = PostgresDb.BuildDb()
                use  conn   = mkConn db
                let! before = conn.countAll PostgresDb.TableName
                Expect.equal before 0 "There should have been no documents returned"
                
                // This not raising an exception is the test
                do! conn.updatePartialById PostgresDb.TableName "test" {| Foo = "green" |}
            }
        ]
        testList "updatePartialByField" [
            testTask "succeeds when a document is updated" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn
                
                do! conn.updatePartialByField PostgresDb.TableName "Value" EQ "purple" {| NumValue = 77 |}
                let! after = conn.countByField PostgresDb.TableName "NumValue" EQ "77"
                Expect.equal after 2 "There should have been 2 documents returned"
            }
            testTask "succeeds when no document is updated" {
                use  db     = PostgresDb.BuildDb()
                use  conn   = mkConn db
                let! before = conn.countAll PostgresDb.TableName
                Expect.equal before 0 "There should have been no documents returned"
                
                // This not raising an exception is the test
                do! conn.updatePartialByField PostgresDb.TableName "Value" EQ "burgundy" {| Foo = "green" |}
            }
        ]
        testList "updatePartialByContains" [
            testTask "succeeds when a document is updated" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn
                
                do! conn.updatePartialByContains PostgresDb.TableName {| Value = "purple" |} {| NumValue = 77 |}
                let! after = conn.countByContains PostgresDb.TableName {| NumValue = 77 |}
                Expect.equal after 2 "There should have been 2 documents returned"
            }
            testTask "succeeds when no document is updated" {
                use  db     = PostgresDb.BuildDb()
                use  conn   = mkConn db
                let! before = conn.countAll PostgresDb.TableName
                Expect.equal before 0 "There should have been no documents returned"
                
                // This not raising an exception is the test
                do! conn.updatePartialByContains PostgresDb.TableName {| Value = "burgundy" |} {| Foo = "green" |}
            }
        ]
        testList "updatePartialByJsonPath" [
            testTask "succeeds when a document is updated" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn
                
                do! conn.updatePartialByJsonPath PostgresDb.TableName "$.NumValue ? (@ > 10)" {| NumValue = 1000 |}
                let! after = conn.countByJsonPath PostgresDb.TableName "$.NumValue ? (@ > 999)"
                Expect.equal after 2 "There should have been 2 documents returned"
            }
            testTask "succeeds when no document is updated" {
                use  db     = PostgresDb.BuildDb()
                use  conn   = mkConn db
                let! before = conn.countAll PostgresDb.TableName
                Expect.equal before 0 "There should have been no documents returned"
                
                // This not raising an exception is the test
                do! conn.updatePartialByJsonPath PostgresDb.TableName "$.NumValue ? (@ < 0)" {| Foo = "green" |}
            }
        ]
        testList "deleteById" [
            testTask "succeeds when a document is deleted" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                do! conn.deleteById PostgresDb.TableName "four"
                let! remaining = conn.countAll PostgresDb.TableName
                Expect.equal remaining 4 "There should have been 4 documents remaining"
            }
            testTask "succeeds when a document is not deleted" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                do! conn.deleteById PostgresDb.TableName "thirty"
                let! remaining = conn.countAll PostgresDb.TableName
                Expect.equal remaining 5 "There should have been 5 documents remaining"
            }
        ]
        testList "deleteByField" [
            testTask "succeeds when documents are deleted" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                do! conn.deleteByField PostgresDb.TableName "Value" EQ "purple"
                let! remaining = conn.countAll PostgresDb.TableName
                Expect.equal remaining 3 "There should have been 3 documents remaining"
            }
            testTask "succeeds when documents are not deleted" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                do! conn.deleteByField PostgresDb.TableName "Value" EQ "crimson"
                let! remaining = conn.countAll PostgresDb.TableName
                Expect.equal remaining 5 "There should have been 5 documents remaining"
            }
        ]
        testList "deleteByContains" [
            testTask "succeeds when documents are deleted" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                do! conn.deleteByContains PostgresDb.TableName {| Value = "purple" |}
                let! remaining = conn.countAll PostgresDb.TableName
                Expect.equal remaining 3 "There should have been 3 documents remaining"
            }
            testTask "succeeds when documents are not deleted" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                do! conn.deleteByContains PostgresDb.TableName {| Value = "crimson" |}
                let! remaining = conn.countAll PostgresDb.TableName
                Expect.equal remaining 5 "There should have been 5 documents remaining"
            }
        ]
        testList "deleteByJsonPath" [
            testTask "succeeds when documents are deleted" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                do! conn.deleteByJsonPath PostgresDb.TableName """$.Sub.Foo ? (@ == "green")"""
                let! remaining = conn.countAll PostgresDb.TableName
                Expect.equal remaining 3 "There should have been 3 documents remaining"
            }
            testTask "succeeds when documents are not deleted" {
                use db   = PostgresDb.BuildDb()
                use conn = mkConn db
                do! loadDocs conn

                do! conn.deleteByJsonPath PostgresDb.TableName "$.NumValue ? (@ > 100)"
                let! remaining = conn.countAll PostgresDb.TableName
                Expect.equal remaining 5 "There should have been 5 documents remaining"
            }
        ]
    ]
    |> testSequenced
