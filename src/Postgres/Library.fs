namespace BitBadger.Documents.Postgres

/// The type of index to generate for the document
[<Struct>]
type DocumentIndex =
    /// A GIN index with standard operations (all operators supported)
    | Full
    /// A GIN index with JSONPath operations (optimized for @>, @?, @@ operators)
    | Optimized


/// Configuration for document handling
module Configuration =

    /// The data source to use for query execution
    let mutable private dataSourceValue : Npgsql.NpgsqlDataSource option = None

    /// Register a data source to use for query execution (disposes the current one if it exists)
    let useDataSource source =
        if Option.isSome dataSourceValue then dataSourceValue.Value.Dispose()
        dataSourceValue <- Some source
    
    /// Retrieve the currently configured data source
    let dataSource () =
        match dataSourceValue with
        | Some source -> source
        | None -> invalidOp "Please provide a data source before attempting data access"
    

open Npgsql.FSharp

/// Helper functions
[<AutoOpen>]
module private Helpers =
    /// Shorthand to retrieve the data source as SqlProps
    let internal fromDataSource () =
        Configuration.dataSource () |> Sql.fromDataSource

    open System.Threading.Tasks

    /// Execute a task and ignore the result
    let internal ignoreTask<'T> (it : Task<'T>) = backgroundTask {
        let! _ = it
        ()
    }


open BitBadger.Documents

/// Data definition
[<RequireQualifiedAccess>]
module Definition =

    /// SQL statement to create a document table
    let createTable name =
        $"CREATE TABLE IF NOT EXISTS %s{name} (data JSONB NOT NULL)"
    
    /// SQL statement to create a key index for a document table
    let createKey (name : string) =
        let tableName = name.Split(".") |> Array.last
        $"CREATE UNIQUE INDEX IF NOT EXISTS idx_{tableName}_key ON {name} ((data ->> '{Configuration.idField ()}'))"
        
    /// SQL statement to create an index on documents in the specified table
    let createIndex (name : string) idxType =
        let extraOps = match idxType with Full -> "" | Optimized -> " jsonb_path_ops"
        let tableName = name.Split(".") |> Array.last
        $"CREATE INDEX IF NOT EXISTS idx_{tableName} ON {name} USING GIN (data{extraOps})"
    
    /// Definitions that take SqlProps as their last parameter
    module WithProps =
        
        /// Create a document table
        let ensureTable name sqlProps = backgroundTask {
            do! sqlProps |> Sql.query (createTable name) |> Sql.executeNonQueryAsync |> ignoreTask
            do! sqlProps |> Sql.query (createKey   name) |> Sql.executeNonQueryAsync |> ignoreTask
        }

        /// Create an index on documents in the specified table
        let ensureIndex name idxType sqlProps =
            sqlProps |> Sql.query (createIndex name idxType) |> Sql.executeNonQueryAsync |> ignoreTask
    
    /// Create a document table
    let ensureTable name =
        WithProps.ensureTable name (fromDataSource ())
    
    let ensureIndex name idxType =
        WithProps.ensureIndex name idxType (fromDataSource ())


/// Query construction functions
[<RequireQualifiedAccess>]
module Query =
    
    /// Create a SELECT clause to retrieve the document data from the given table
    let selectFromTable tableName =
        $"SELECT data FROM %s{tableName}"
    
    /// Create a WHERE clause fragment to implement an ID-based query
    let whereById paramName =
        $"data ->> '{Configuration.idField ()}' = %s{paramName}"
    
    /// Create a WHERE clause fragment to implement a @> (JSON contains) condition
    let whereDataContains paramName =
        $"data @> %s{paramName}"
    
    /// Create a WHERE clause fragment to implement a @? (JSON Path match) condition
    let whereJsonPathMatches paramName =
        $"data @? %s{paramName}::jsonpath"
    
    /// Create a JSONB document parameter
    let jsonbDocParam (it: obj) =
        Sql.jsonb (Configuration.serializer().Serialize it)

    /// Create ID and data parameters for a query
    let docParameters<'T> docId (doc: 'T) =
        [ "@id", Sql.string docId; "@data", jsonbDocParam doc ]
    
    /// Query to insert a document
    let insert tableName =
        $"INSERT INTO %s{tableName} VALUES (@data)"

    /// Query to save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
    let save tableName =
        sprintf "INSERT INTO %s VALUES (@data) ON CONFLICT ((data ->> '%s')) DO UPDATE SET data = EXCLUDED.data"
                tableName (Configuration.idField ()) 
    
    /// Queries for counting documents
    module Count =
        
        /// Query to count all documents in a table
        let all tableName =
            $"SELECT COUNT(*) AS it FROM %s{tableName}"
        
        /// Query to count matching documents using a JSON containment query (@>)
        let byContains tableName =
            $"""SELECT COUNT(*) AS it FROM %s{tableName} WHERE {whereDataContains "@criteria"}"""
        
        /// Query to count matching documents using a JSON Path match (@?)
        let byJsonPath tableName =
            $"""SELECT COUNT(*) AS it FROM %s{tableName} WHERE {whereJsonPathMatches "@path"}"""
    
    /// Queries for determining document existence
    module Exists =

        /// Query to determine if a document exists for the given ID
        let byId tableName =
            $"""SELECT EXISTS (SELECT 1 FROM %s{tableName} WHERE {whereById "@id"}) AS it"""

        /// Query to determine if documents exist using a JSON containment query (@>)
        let byContains tableName =
            $"""SELECT EXISTS (SELECT 1 FROM %s{tableName} WHERE {whereDataContains "@criteria"}) AS it"""
        
        /// Query to determine if documents exist using a JSON Path match (@?)
        let byJsonPath tableName =
            $"""SELECT EXISTS (SELECT 1 FROM %s{tableName} WHERE {whereJsonPathMatches "@path"}) AS it"""
    
    /// Queries for retrieving documents
    module Find =

        /// Query to retrieve a document by its ID
        let byId tableName =
            $"""{selectFromTable tableName} WHERE {whereById "@id"}"""
        
        /// Query to retrieve documents using a JSON containment query (@>)
        let byContains tableName =
            $"""{selectFromTable tableName} WHERE {whereDataContains "@criteria"}"""
        
        /// Query to retrieve documents using a JSON Path match (@?)
        let byJsonPath tableName =
            $"""{selectFromTable tableName} WHERE {whereJsonPathMatches "@path"}"""
    
    /// Queries to update documents
    module Update =

        /// Query to update a document
        let full tableName =
            $"""UPDATE %s{tableName} SET data = @data WHERE {whereById "@id"}"""

        /// Query to update a document
        let partialById tableName =
            $"""UPDATE %s{tableName} SET data = data || @data WHERE {whereById "@id"}"""
        
        /// Query to update partial documents matching a JSON containment query (@>)
        let partialByContains tableName =
            $"""UPDATE %s{tableName} SET data = data || @data WHERE {whereDataContains "@criteria"}"""

        /// Query to update partial documents matching a JSON containment query (@>)
        let partialByJsonPath tableName =
            $"""UPDATE %s{tableName} SET data = data || @data WHERE {whereJsonPathMatches "@path"}"""

    /// Queries to delete documents
    module Delete =
        
        /// Query to delete a document by its ID
        let byId tableName =
            $"""DELETE FROM %s{tableName} WHERE {whereById "@id"}"""

        /// Query to delete documents using a JSON containment query (@>)
        let byContains tableName =
            $"""DELETE FROM %s{tableName} WHERE {whereDataContains "@criteria"}"""

        /// Query to delete documents using a JSON Path match (@?)
        let byJsonPath tableName =
            $"""DELETE FROM %s{tableName} WHERE {whereJsonPathMatches "@path"}"""


/// Functions for dealing with results
[<AutoOpen>]
module Results =
    
    /// Create a domain item from a document, specifying the field in which the document is found
    let fromDocument<'T> field (row: RowReader) : 'T =
        Configuration.serializer().Deserialize<'T>(row.string field)
        
    /// Create a domain item from a document
    let fromData<'T> row : 'T =
        fromDocument "data" row


/// Versions of queries that accept SqlProps as the last parameter
module WithProps =
    
    /// Execute a non-query statement to manipulate a document
    let private executeNonQuery query (document: 'T) sqlProps =
        sqlProps
        |> Sql.query query
        |> Sql.parameters [ "@data", Query.jsonbDocParam document ]
        |> Sql.executeNonQueryAsync
        |> ignoreTask

    /// Execute a non-query statement to manipulate a document with an ID specified
    let private executeNonQueryWithId query docId (document: 'T) sqlProps =
        sqlProps
        |> Sql.query query
        |> Sql.parameters (Query.docParameters docId document)
        |> Sql.executeNonQueryAsync
        |> ignoreTask

    /// Insert a new document
    let insert<'T> tableName (document: 'T) sqlProps =
        executeNonQuery (Query.insert tableName) document sqlProps

    /// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
    let save<'T> tableName (document: 'T) sqlProps =
        executeNonQuery (Query.save tableName) document sqlProps

    /// Commands to count documents
    [<RequireQualifiedAccess>]
    module Count =
        
        /// Count all documents in a table
        let all tableName sqlProps =
            sqlProps
            |> Sql.query (Query.Count.all tableName)
            |> Sql.executeRowAsync (fun row -> row.int "it")
        
        /// Count matching documents using a JSON containment query (@>)
        let byContains tableName (criteria: obj) sqlProps =
            sqlProps
            |> Sql.query (Query.Count.byContains tableName)
            |> Sql.parameters [ "@criteria", Query.jsonbDocParam criteria ]
            |> Sql.executeRowAsync (fun row -> row.int "it")

        /// Count matching documents using a JSON Path match query (@?)
        let byJsonPath tableName jsonPath sqlProps =
            sqlProps
            |> Sql.query (Query.Count.byJsonPath tableName)
            |> Sql.parameters [ "@path", Sql.string jsonPath ]
            |> Sql.executeRowAsync (fun row -> row.int "it")
    
    /// Commands to determine if documents exist
    [<RequireQualifiedAccess>]
    module Exists =

        /// Determine if a document exists for the given ID
        let byId tableName docId sqlProps =
            sqlProps
            |> Sql.query (Query.Exists.byId tableName)
            |> Sql.parameters [ "@id", Sql.string docId ]
            |> Sql.executeRowAsync (fun row -> row.bool "it")

        /// Determine if a document exists using a JSON containment query (@>)
        let byContains tableName (criteria: obj) sqlProps =
            sqlProps
            |> Sql.query (Query.Exists.byContains tableName)
            |> Sql.parameters [ "@criteria", Query.jsonbDocParam criteria ]
            |> Sql.executeRowAsync (fun row -> row.bool "it")

        /// Determine if a document exists using a JSON Path match query (@?)
        let byJsonPath tableName jsonPath sqlProps =
            sqlProps
            |> Sql.query (Query.Exists.byJsonPath tableName)
            |> Sql.parameters [ "@path", Sql.string jsonPath ]
            |> Sql.executeRowAsync (fun row -> row.bool "it")

    /// Commands to determine if documents exist
    [<RequireQualifiedAccess>]
    module Find =
        
        /// Retrieve all documents in the given table
        let all<'T> tableName sqlProps =
            sqlProps
            |> Sql.query (Query.selectFromTable tableName)
            |> Sql.executeAsync fromData<'T>

        /// Retrieve a document by its ID
        let byId<'T> tableName docId sqlProps = backgroundTask {
            let! results =
                sqlProps
                |> Sql.query (Query.Find.byId tableName)
                |> Sql.parameters [ "@id", Sql.string docId ]
                |> Sql.executeAsync fromData<'T>
            return List.tryHead results
        }

        /// Execute a JSON containment query (@>)
        let byContains<'T> tableName (criteria: obj) sqlProps =
            sqlProps
            |> Sql.query (Query.Find.byContains tableName)
            |> Sql.parameters [ "@criteria", Query.jsonbDocParam criteria ]
            |> Sql.executeAsync fromData<'T>

        /// Execute a JSON Path match query (@?)
        let byJsonPath<'T> tableName jsonPath sqlProps =
            sqlProps
            |> Sql.query (Query.Find.byJsonPath tableName)
            |> Sql.parameters [ "@path", Sql.string jsonPath ]
            |> Sql.executeAsync fromData<'T>
        
        /// Execute a JSON containment query (@>), returning only the first result
        let firstByContains<'T> tableName (criteria: obj) sqlProps = backgroundTask {
            let! results = byContains<'T> tableName criteria sqlProps
            return List.tryHead results
        }

        /// Execute a JSON Path match query (@?), returning only the first result
        let firstByJsonPath<'T> tableName jsonPath sqlProps = backgroundTask {
            let! results = byJsonPath<'T> tableName jsonPath sqlProps
            return List.tryHead results
        }

    /// Commands to update documents
    [<RequireQualifiedAccess>]
    module Update =
        
        /// Update an entire document
        let full<'T> tableName docId (document: 'T) sqlProps =
            executeNonQueryWithId (Query.Update.full tableName) docId document sqlProps
        
        /// Update an entire document
        let fullFunc<'T> tableName (idFunc: 'T -> string) (document: 'T) sqlProps =
            full tableName (idFunc document) document sqlProps
        
        /// Update a partial document
        let partialById tableName docId (partial: obj) sqlProps =
            executeNonQueryWithId (Query.Update.partialById tableName) docId partial sqlProps
        
        /// Update partial documents using a JSON containment query in the WHERE clause (@>)
        let partialByContains tableName (criteria: obj) (partial: obj) sqlProps =
            sqlProps
            |> Sql.query (Query.Update.partialByContains tableName)
            |> Sql.parameters [ "@data", Query.jsonbDocParam partial; "@criteria", Query.jsonbDocParam criteria ]
            |> Sql.executeNonQueryAsync
            |> ignoreTask 
        
        /// Update partial documents using a JSON Path match query in the WHERE clause (@?)
        let partialByJsonPath tableName jsonPath (partial: obj) sqlProps =
            sqlProps
            |> Sql.query (Query.Update.partialByJsonPath tableName)
            |> Sql.parameters [ "@data", Query.jsonbDocParam partial; "@path", Sql.string jsonPath ]
            |> Sql.executeNonQueryAsync
            |> ignoreTask 

    /// Commands to delete documents
    [<RequireQualifiedAccess>]
    module Delete =
        
        /// Delete a document by its ID
        let byId tableName docId sqlProps =
            executeNonQueryWithId (Query.Delete.byId tableName) docId {||} sqlProps

        /// Delete documents by matching a JSON contains query (@>)
        let byContains tableName (criteria: obj) sqlProps =
            sqlProps
            |> Sql.query (Query.Delete.byContains tableName)
            |> Sql.parameters [ "@criteria", Query.jsonbDocParam criteria ]
            |> Sql.executeNonQueryAsync
            |> ignoreTask

        /// Delete documents by matching a JSON Path match query (@?)
        let byJsonPath tableName path sqlProps =
            sqlProps
            |> Sql.query (Query.Delete.byJsonPath tableName)
            |> Sql.parameters [ "@path", Sql.string path ]
            |> Sql.executeNonQueryAsync
            |> ignoreTask
    
    /// Commands to execute custom SQL queries
    [<RequireQualifiedAccess>]
    module Custom =

        /// Execute a query that returns one or no results
        let single<'T> query parameters (mapFunc: RowReader -> 'T) sqlProps = backgroundTask {
            let! results =
                Sql.query query sqlProps
                |> Sql.parameters parameters
                |> Sql.executeAsync mapFunc
            return List.tryHead results
        }

        /// Execute a query that returns a list of results
        let list<'T> query parameters (mapFunc: RowReader -> 'T) sqlProps =
            Sql.query query sqlProps
            |> Sql.parameters parameters
            |> Sql.executeAsync mapFunc

        /// Execute a query that returns no results
        let nonQuery query parameters sqlProps =
            Sql.query query sqlProps
            |> Sql.parameters (List.ofSeq parameters)
            |> Sql.executeNonQueryAsync
            |> ignoreTask
        
        /// Execute a query that returns a scalar value
        let scalar<'T when 'T : struct> query parameters (mapFunc: RowReader -> 'T) sqlProps =
            Sql.query query sqlProps
            |> Sql.parameters parameters
            |> Sql.executeRowAsync mapFunc


/// Document writing functions
[<AutoOpen>]
module Document =
    /// Insert a new document
    let insert<'T> tableName (document: 'T) =
        WithProps.insert tableName document (fromDataSource ())

    /// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
    let save<'T> tableName (document: 'T) =
        WithProps.save<'T> tableName document (fromDataSource ())


/// Queries to count documents
[<RequireQualifiedAccess>]
module Count =
    
    /// Count all documents in a table
    let all tableName =
        WithProps.Count.all tableName (fromDataSource ())
    
    /// Count matching documents using a JSON containment query (@>)
    let byContains tableName criteria =
        WithProps.Count.byContains tableName criteria (fromDataSource ())

    /// Count matching documents using a JSON Path match query (@?)
    let byJsonPath tableName jsonPath =
        WithProps.Count.byJsonPath tableName jsonPath (fromDataSource ())


/// Queries to determine if documents exist
[<RequireQualifiedAccess>]
module Exists =

    /// Determine if a document exists for the given ID
    let byId tableName docId =
        WithProps.Exists.byId tableName docId (fromDataSource ())
    
    /// Determine if a document exists using a JSON containment query (@>)
    let byContains tableName criteria =
        WithProps.Exists.byContains tableName criteria (fromDataSource ())

    /// Determine if a document exists using a JSON Path match query (@?)
    let byJsonPath tableName jsonPath =
        WithProps.Exists.byJsonPath tableName jsonPath (fromDataSource ())


/// Commands to retrieve documents
[<RequireQualifiedAccess>]
module Find =
    
    /// Retrieve all documents in the given table
    let all<'T> tableName =
        WithProps.Find.all<'T> tableName (fromDataSource ())

    /// Retrieve a document by its ID
    let byId<'T> tableName docId =
        WithProps.Find.byId<'T> tableName docId (fromDataSource ())

    /// Execute a JSON containment query (@>)
    let byContains<'T> tableName criteria =
        WithProps.Find.byContains<'T> tableName criteria (fromDataSource ())

    /// Execute a JSON Path match query (@?)
    let byJsonPath<'T> tableName jsonPath =
        WithProps.Find.byJsonPath<'T> tableName jsonPath (fromDataSource ())
    
    /// Execute a JSON containment query (@>), returning only the first result
    let firstByContains<'T> tableName (criteria: obj) =
        WithProps.Find.firstByContains<'T> tableName criteria (fromDataSource ())

    /// Execute a JSON Path match query (@?), returning only the first result
    let firstByJsonPath<'T> tableName jsonPath =
        WithProps.Find.firstByJsonPath<'T> tableName jsonPath (fromDataSource ())


/// Commands to update documents
[<RequireQualifiedAccess>]
module Update =

    /// Update a full document
    let full<'T> tableName docId (document: 'T) =
        WithProps.Update.full<'T> tableName docId document (fromDataSource ())

    /// Update a full document
    let fullFunc<'T> tableName idFunc (document: 'T) =
        WithProps.Update.fullFunc<'T> tableName idFunc document (fromDataSource ())

    /// Update a partial document
    let partialById tableName docId (partial: obj) =
        WithProps.Update.partialById tableName docId partial (fromDataSource ())
    
    /// Update partial documents using a JSON containment query in the WHERE clause (@>)
    let partialByContains tableName (criteria: obj) (partial: obj) =
        WithProps.Update.partialByContains tableName criteria partial (fromDataSource ())
    
    /// Update partial documents using a JSON Path match query in the WHERE clause (@?)
    let partialByJsonPath tableName jsonPath (partial: obj) =
        WithProps.Update.partialByJsonPath tableName jsonPath partial (fromDataSource ())


/// Commands to delete documents
[<RequireQualifiedAccess>]
module Delete =
    
    /// Delete a document by its ID
    let byId tableName docId =
        WithProps.Delete.byId tableName docId (fromDataSource ())

    /// Delete documents by matching a JSON contains query (@>)
    let byContains tableName (criteria: obj) =
        WithProps.Delete.byContains tableName criteria (fromDataSource ())

    /// Delete documents by matching a JSON Path match query (@?)
    let byJsonPath tableName path =
        WithProps.Delete.byJsonPath tableName path (fromDataSource ())

    
/// Commands to execute custom SQL queries
[<RequireQualifiedAccess>]
module Custom =

    /// Execute a query that returns one or no results
    let single<'T> query parameters (mapFunc: RowReader ->  'T) =
        WithProps.Custom.single query parameters mapFunc (fromDataSource ())

    /// Execute a query that returns a list of results
    let list<'T> query parameters (mapFunc: RowReader -> 'T) =
        WithProps.Custom.list query parameters mapFunc (fromDataSource ())

    /// Execute a query that returns no results
    let nonQuery query parameters =
        WithProps.Custom.nonQuery query parameters (fromDataSource ())

    /// Execute a query that returns a scalar value
    let scalar<'T when 'T: struct> query parameters (mapFunc: RowReader -> 'T) =
        WithProps.Custom.scalar query parameters mapFunc (fromDataSource ())
