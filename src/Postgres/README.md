# BitBadger.Documents.Postgres

This package provides a lightweight document library backed by [PostgreSQL](https://www.postgresql.org). It also provides streamlined functions for traditional ADO.NET functionality where relational data is required. Both C# and F# have first-class implementations.

## Features

- Select, insert, update, save (upsert), delete, count, and check existence of documents, and create tables and indexes for these documents
- Address documents via ID, via comparison on any field, via equality on any property (using JSON containment, on a likely indexed field), or via condition on any property (using JSON Path queries)
- Access documents as your domain models (<abbr title="Plain Old CLR Objects">POCO</abbr>s)
- Use `Task`-based async for all data access functions
- Use building blocks for more complex queries

## Getting Started

Once the package is installed, the library needs a data source. Construct an `NpgsqlDataSource` instance, and provide it to the library:

```csharp
// C#
using BitBadger.Documents.Postgres;

//...
// Do not use "using" here; the library will handle disposing this instance
var data = new NpgsqlDataSourceBuilder("connection-string").Build();
Postgres.Configuration.UseDataSource(data);
```

```fsharp
// F#
open BitBadger.Documents.Postgres

// ...
// Do not use "use" here; the library will handle disposing this instance
let dataSource = // same as above ....

Configuration.useDataSource dataSource
// ...
```

By default, the library uses a `System.Text.Json`-based serializer configured to use the `FSharp.SystemTextJson` converter. To provide a different serializer (different options, more converters, etc.), construct it to implement `IDocumentSerializer` and provide it via `Configuration.useSerializer`. If custom serialization makes the serialized Id field not be `Id`, that will also need to be configured.

## Using

Retrieve all customers:

```csharp
// C#; parameter is table name
// Find.All type signature is Func<string, Task<List<TDoc>>>
var customers = await Find.All("customer");
```

```fsharp
// F#
// Find.all type signature is string -> Task<'TDoc list>
let! customers = Find.all<Customer> "customer"
```

Select a customer by ID:

```csharp
// C#; parameters are table name and ID
// Find.ById type signature is Func<string, TKey, Task<TDoc?>>
var customer = await Find.ById<string, Customer>("customer", "123");
```
```fsharp
// F#
// Find.byId type signature is string -> 'TKey -> Task<'TDoc option>
let! customer = Find.byId<string, Customer> "customer" "123"
```
_(keys are treated as strings in the database)_

Count customers in Atlanta (using JSON containment):

```csharp
// C#; parameters are table name and object for containment query
// Count.ByContains type signature is Func<string, TCriteria, int>
var customerCount = await Count.ByContains("customer", new { City = "Atlanta" });
```

```fsharp
// F#
// Count.byContains type signature is string -> 'TCriteria -> Task<int>
let! customerCount = Count.byContains "customer" {| City = "Atlanta" |}
```

Delete customers in Chicago: _(no offense, Second City; just an example...)_

```csharp
// C#; parameters are table name and JSON Path expression
// Delete.ByJsonPath type signature is Func<string, string, Task>
await Delete.ByJsonPath("customer", "$.City ? (@ == \"Chicago\")");
```

```fsharp
// F#
// Delete.byJsonPath type signature is string -> string -> Task<unit>
do! Delete.byJsonPath "customer" """$.City ? (@ == "Chicago")"""
```

## More Information

The [project site](https://bitbadger.solutions/open-source/relational-documents/) has full details on how to use this library.
