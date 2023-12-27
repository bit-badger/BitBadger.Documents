open Expecto
open BitBadger.Documents.Tests.CSharp

let allTests =
    testList
        "BitBadger.Documents"
        [ CommonTests.all
          CommonCSharpTests.Unit
          PostgresTests.all
          PostgresCSharpTests.All
          PostgresExtensionTests.integrationTests
          testSequenced PostgresCSharpExtensionTests.Integration
          SqliteTests.all
          SqliteCSharpTests.All
          SqliteExtensionTests.integrationTests
          testSequenced SqliteCSharpExtensionTests.Integration ]

[<EntryPoint>]
let main args = runTestsWithCLIArgs [] args allTests
