open Expecto
open BitBadger.Documents.Tests.CSharp

let allTests =
    testList
        "BitBadger.Documents"
        [ CommonTests.all
          CommonCSharpTests.Unit
          SqliteTests.all
          testSequenced SqliteExtensionTests.integrationTests
          SqliteCSharpTests.All
          testSequenced SqliteCSharpExtensionTests.Integration ]

[<EntryPoint>]
let main args = runTestsWithCLIArgs [] args allTests
