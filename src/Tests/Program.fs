open Expecto

let allTests = testList "BitBadger.Documents" [ CommonTests.all; SqliteTests.all ]

[<EntryPoint>]
let main args = runTestsWithCLIArgs [] args allTests
