open Expecto
open BitBadger.Documents.Tests.CSharp

let allTests =
    testList
        "BitBadger.Documents"
        [ CommonTests.all
          CommonCSharpTests.Unit
          SqliteTests.all
          testSequenced SqliteCSharpTests.Integration ]

[<EntryPoint>]
let main args = runTestsWithCLIArgs [] args allTests
