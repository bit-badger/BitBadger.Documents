module CommonTests

open BitBadger.Documents
open Expecto

/// Test table name
let tbl = "test_table"

/// Tests which do not hit the database
let all =
    testList "Common" [
        testList "Op" [
            test "EQ succeeds" {
                Expect.equal (string EQ) "=" "The equals operator was not correct"
            }
            test "GT succeeds" {
                Expect.equal (string GT) ">" "The greater than operator was not correct"
            }
            test "GE succeeds" {
                Expect.equal (string GE) ">=" "The greater than or equal to operator was not correct"
            }
            test "LT succeeds" {
                Expect.equal (string LT) "<" "The less than operator was not correct"
            }
            test "LE succeeds" {
                Expect.equal (string LE) "<=" "The less than or equal to operator was not correct"
            }
            test "NE succeeds" {
                Expect.equal (string NE) "<>" "The not equal to operator was not correct"
            }
            test "EX succeeds" {
                Expect.equal (string EX) "IS NOT NULL" """The "exists" operator ws not correct"""
            }
            test "NEX succeeds" {
                Expect.equal (string NEX) "IS NULL" """The "not exists" operator ws not correct"""
            }
        ]
        testList "Query" [
            test "selectFromTable succeeds" {
                Expect.equal (Query.selectFromTable tbl) $"SELECT data FROM {tbl}" "SELECT statement not correct"
            }
            test "whereById succeeds" {
                Expect.equal (Query.whereById "@id") "data ->> 'Id' = @id" "WHERE clause not correct"
            }
            testList "whereByField" [
                test "succeeds when a logical operator is passed" {
                    Expect.equal
                        (Query.whereByField "theField" GT "@test")
                        "data ->> 'theField' > @test"
                        "WHERE clause not correct"
                }
                test "succeeds when an existence operator is passed" {
                    Expect.equal
                        (Query.whereByField "thatField" NEX "")
                        "data ->> 'thatField' IS NULL"
                        "WHERE clause not correct"
                }
            ]
            test "insert succeeds" {
                Expect.equal (Query.insert tbl) $"INSERT INTO {tbl} VALUES (@data)" "INSERT statement not correct"
            }
            test "save succeeds" {
                Expect.equal
                    (Query.save tbl)
                    $"INSERT INTO {tbl} VALUES (@data) ON CONFLICT ((data ->> 'Id')) DO UPDATE SET data = EXCLUDED.data"
                    "INSERT ON CONFLICT UPDATE statement not correct"
            }
            testList "Count" [
                test "all succeeds" {
                    Expect.equal (Query.Count.all tbl) $"SELECT COUNT(*) AS it FROM {tbl}" "Count query not correct"
                }
                test "byField succeeds" {
                    Expect.equal
                        (Query.Count.byField tbl "thatField" EQ)
                        $"SELECT COUNT(*) AS it FROM {tbl} WHERE data ->> 'thatField' = @field"
                        "JSON field text comparison count query not correct"
                }
            ]
            testList "Exists" [
                test "byId succeeds" {
                    Expect.equal
                        (Query.Exists.byId tbl)
                        $"SELECT EXISTS (SELECT 1 FROM {tbl} WHERE data ->> 'Id' = @id) AS it"
                        "ID existence query not correct"
                }
                test "byField succeeds" {
                    Expect.equal
                        (Query.Exists.byField tbl "Test" LT)
                        $"SELECT EXISTS (SELECT 1 FROM {tbl} WHERE data ->> 'Test' < @field) AS it"
                        "JSON field text comparison exists query not correct"
                }
            ]
            testList "Find" [
                test "byId succeeds" {
                    Expect.equal
                        (Query.Find.byId tbl)
                        $"SELECT data FROM {tbl} WHERE data ->> 'Id' = @id"
                        "SELECT by ID query not correct"
                }
                test "byField succeeds" {
                    Expect.equal
                        (Query.Find.byField tbl "Golf" GE)
                        $"SELECT data FROM {tbl} WHERE data ->> 'Golf' >= @field"
                        "SELECT by JSON comparison query not correct"
                }
            ]
            testList "Update" [
                test "full succeeds" {
                    Expect.equal
                        (Query.Update.full tbl)
                        $"UPDATE {tbl} SET data = @data WHERE data ->> 'Id' = @id"
                        "UPDATE full statement not correct"
                }
                test "partialById succeeds" {
                    Expect.equal
                        (Query.Update.partialById tbl)
                        $"UPDATE {tbl} SET data = json_patch(data, json(@data)) WHERE data ->> 'Id' = @id"
                        "UPDATE partial by ID statement not correct"
                }
                test "partialByField succeeds" {
                    Expect.equal
                        (Query.Update.partialByField tbl "Part" NE)
                        $"UPDATE {tbl} SET data = json_patch(data, json(@data)) WHERE data ->> 'Part' <> @field"
                        "UPDATE partial by JSON comparison query not correct"
                }
            ]
            testList "Delete" [
                test "byId succeeds" {
                    Expect.equal
                        (Query.Delete.byId tbl)
                        $"DELETE FROM {tbl} WHERE data ->> 'Id' = @id"
                        "DELETE by ID query not correct"
                }
                test "byField succeeds" {
                    Expect.equal
                        (Query.Delete.byField tbl "gone" NEX)
                        $"DELETE FROM {tbl} WHERE data ->> 'gone' IS NULL"
                        "DELETE by JSON comparison query not correct"
                }
            ]
        ]
    ]

