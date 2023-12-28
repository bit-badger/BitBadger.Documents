# BitBadger.Documents.Common

This package provides common definitions and functionality for `BitBadger.Documents` implementations. These libraries provide a document storage view over relational databases, while also providing convenience functions for relational usage as well. This enables a hybrid approach to data storage, allowing the user to use documents where they make sense, while streamlining traditional ADO.NET functionality where relational data is required.
- `BitBadger.Documents.Postgres` ([NuGet](https://www.nuget.org/packages/BitBadger.Documents.Postgres/)) provides a PostgreSQL implementation.
- `BitBadger.Documents.Sqlite` ([NuGet](https://www.nuget.org/packages/BitBadger.Documents.Sqlite/)) provides a SQLite implementation

## Features

- Select, insert, update, save (upsert), delete, count, and check existence of documents, and create tables and indexes for these documents
- Addresses documents via ID and via comparison on any field (for PostgreSQL, also via equality on any property by using JSON containment, or via condition on any property using JSON Path queries)
- Accesses documents as your domain models (<abbr title="Plain Old CLR Objects">POCO</abbr>s)
- Uses `Task`-based async for all data access functions
- Uses building blocks for more complex queries

## Getting Started

Install the library of your choice and follow its README; also, the [project site](https://bitbadger.solutions/open-source/relational-documents/) has complete documentation.
