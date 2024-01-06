module Types

type SubDocument =
    { Foo: string
      Bar: string }

type JsonDocument =
    { Id: string
      Value: string
      NumValue: int
      Sub: SubDocument option }

/// An empty JsonDocument
let emptyDoc = { Id = ""; Value = ""; NumValue = 0; Sub = None }

/// Documents to use for testing
let testDocuments = [
    { Id = "one"; Value = "FIRST!"; NumValue = 0; Sub = None }
    { Id = "two"; Value = "another"; NumValue = 10; Sub = Some { Foo = "green"; Bar = "blue" } }
    { Id = "three"; Value = ""; NumValue = 4; Sub = None }
    { Id = "four"; Value = "purple"; NumValue = 17; Sub = Some { Foo = "green"; Bar = "red" } }
    { Id = "five"; Value = "purple"; NumValue = 18; Sub = None }
]
