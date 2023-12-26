module Types

type SubDocument =
    { Foo: string
      Bar: string }

type JsonDocument =
    { Id: string
      Value: string
      NumValue: int
      Sub: SubDocument option }

let emptyDoc = { Id = ""; Value = ""; NumValue = 0; Sub = None }
