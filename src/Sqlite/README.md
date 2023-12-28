# BitBadger.Documents.Sqlite

This package provides a lightweight document library backed by [SQLite](https://www.sqlite.org). It also provides streamlined functions for traditional ADO.NET functionality where relational data is required. Both C# and F# have first-class implementations.

## Features

- Select, insert, update, save (upsert), delete, count, and check existence of documents, and create tables and indexes for these documents
- Address documents via ID or via comparison on any field
- Access documents as your domain models (<abbr title="Plain Old CLR Objects">POCO</abbr>s)
- Use `Task`-based async for all data access functions
- Use building blocks for more complex queries

## Getting Started

Once the package is installed, the library needs a connection string. Once it has been obtained / constructed, provide it to the library:

```csharp
// C#
using BitBadger.Documents.Sqlite;

//...
Sqlite.Configuration.UseConnectionString("connection-string");

// A new, open connection to the database can be obtained via
// Sqlite.Configuration.DbConn()
```

```fsharp
// F#
open BitBadger.Documents.Sqlite

// ...
Configuration.useConnectionString "connection-string"

// A new, open connection to the database can be obtained via
// Configuration.dbConn ()
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

Count customers in Atlanta:

```csharp
// C#; parameters are table name, field, operator, and value
// Count.ByField type signature is Func<string, string, Op, object, Task<long>>
var customerCount = await Count.ByField("customer", "City", Op.EQ, "Atlanta");
```

```fsharp
// F#
// Count.byField type signature is string -> string -> Op -> obj -> Task<int64>
let! customerCount = Count.byField "customer" "City" EQ "Atlanta"
```

Delete customers in Chicago: _(no offense, Second City; just an example...)_

```csharp
// C#; parameters are same as above, except return is void
// Delete.ByField type signature is Func<string, string, Op, object, Task>
await Delete.ByField("customer", "City", Op.EQ, "Chicago");
```

```fsharp
// F#
// Delete.byField type signature is string -> string -> Op -> obj -> Task<unit>
do! Delete.byField "customer" "City" EQ "Chicago"
```

## More Information

The [project site](https://bitbadger.solutions/open-source/relational-documents/) has full details on how to use this library.
