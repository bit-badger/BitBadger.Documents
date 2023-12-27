namespace BitBadger.Documents.Postgres

/// The type of index to generate for the document
[<Struct>]
type DocumentIndex =
    /// A GIN index with standard operations (all operators supported)
    | Full
    /// A GIN index with JSONPath operations (optimized for @>, @?, @@ operators)
    | Optimized


open Npgsql

/// Configuration for document handling
module Configuration =

    /// The data source to use for query execution
    let mutable private dataSourceValue : NpgsqlDataSource option = None

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

    /// Execute a task and ignore the result
    let internal ignoreTask<'T> (it : System.Threading.Tasks.Task<'T>) = backgroundTask {
        let! _ = it
        ()
    }


open BitBadger.Documents

/// Functions for creating parameters
[<AutoOpen>]
module Parameters =
    
    /// Create an ID parameter (name "@id", key will be treated as a string)
    [<CompiledName "Id">]
    let idParam (key: 'TKey) =
        "@id", Sql.string (string key)

    /// Create a parameter with a JSON value
    [<CompiledName "Json">]
    let jsonParam (name: string) (it: 'TJson) =
        name, Sql.jsonb (Configuration.serializer().Serialize it)

    /// Create a JSON field parameter (name "@field")
    [<CompiledName "Field">]
    let fieldParam (value: obj) =
        "@field", Sql.parameter (NpgsqlParameter("@field", value))

    /// An empty parameter sequence
    [<CompiledName "None">]
    let noParams =
        Seq.empty<string * SqlValue>

    
/// Query construction functions
[<RequireQualifiedAccess>]
module Query =
    
    /// Table and index definition queries
    module Definition =
        
        /// SQL statement to create a document table
        [<CompiledName "EnsureTable">]
        let ensureTable name =
            Query.Definition.ensureTableFor name "JSONB"
        
        /// SQL statement to create an index on JSON documents in the specified table
        [<CompiledName "EnsureJsonIndex">]
        let ensureJsonIndex (name: string) idxType =
            let extraOps = match idxType with Full -> "" | Optimized -> " jsonb_path_ops"
            let tableName = name.Split '.' |> Array.last
            $"CREATE INDEX IF NOT EXISTS idx_{tableName}_document ON {name} USING GIN (data{extraOps})"

    /// Create a WHERE clause fragment to implement a @> (JSON contains) condition
    [<CompiledName "WhereDataContains">]
    let whereDataContains paramName =
        $"data @> %s{paramName}"
    
    /// Create a WHERE clause fragment to implement a @? (JSON Path match) condition
    [<CompiledName "WhereJsonPathMatches">]
    let whereJsonPathMatches paramName =
        $"data @? %s{paramName}::jsonpath"
    
    /// Queries for counting documents
    module Count =
        
        /// Query to count matching documents using a JSON containment query (@>)
        [<CompiledName "ByContains">]
        let byContains tableName =
            $"""SELECT COUNT(*) AS it FROM %s{tableName} WHERE {whereDataContains "@criteria"}"""
        
        /// Query to count matching documents using a JSON Path match (@?)
        [<CompiledName "ByJsonPath">]
        let byJsonPath tableName =
            $"""SELECT COUNT(*) AS it FROM %s{tableName} WHERE {whereJsonPathMatches "@path"}"""
    
    /// Queries for determining document existence
    module Exists =

        /// Query to determine if documents exist using a JSON containment query (@>)
        [<CompiledName "ByContains">]
        let byContains tableName =
            $"""SELECT EXISTS (SELECT 1 FROM %s{tableName} WHERE {whereDataContains "@criteria"}) AS it"""
        
        /// Query to determine if documents exist using a JSON Path match (@?)
        [<CompiledName "ByJsonPath">]
        let byJsonPath tableName =
            $"""SELECT EXISTS (SELECT 1 FROM %s{tableName} WHERE {whereJsonPathMatches "@path"}) AS it"""
    
    /// Queries for retrieving documents
    module Find =

        /// Query to retrieve documents using a JSON containment query (@>)
        [<CompiledName "ByContains">]
        let byContains tableName =
            $"""{Query.selectFromTable tableName} WHERE {whereDataContains "@criteria"}"""
        
        /// Query to retrieve documents using a JSON Path match (@?)
        [<CompiledName "ByJsonPath">]
        let byJsonPath tableName =
            $"""{Query.selectFromTable tableName} WHERE {whereJsonPathMatches "@path"}"""
    
    /// Queries to update documents
    module Update =

        /// Query to update a document
        [<CompiledName "PartialById">]
        let partialById tableName =
            $"""UPDATE %s{tableName} SET data = data || @data WHERE {Query.whereById "@id"}"""
        
        /// Query to update a document
        [<CompiledName "PartialByField">]
        let partialByField tableName fieldName op =
            $"""UPDATE %s{tableName} SET data = data || @data WHERE {Query.whereByField fieldName op "@field"}"""
        
        /// Query to update partial documents matching a JSON containment query (@>)
        [<CompiledName "PartialByContains">]
        let partialByContains tableName =
            $"""UPDATE %s{tableName} SET data = data || @data WHERE {whereDataContains "@criteria"}"""

        /// Query to update partial documents matching a JSON containment query (@>)
        [<CompiledName "PartialByJsonPath">]
        let partialByJsonPath tableName =
            $"""UPDATE %s{tableName} SET data = data || @data WHERE {whereJsonPathMatches "@path"}"""

    /// Queries to delete documents
    module Delete =
        
        /// Query to delete documents using a JSON containment query (@>)
        [<CompiledName "ByContains">]
        let byContains tableName =
            $"""DELETE FROM %s{tableName} WHERE {whereDataContains "@criteria"}"""

        /// Query to delete documents using a JSON Path match (@?)
        [<CompiledName "ByJsonPath">]
        let byJsonPath tableName =
            $"""DELETE FROM %s{tableName} WHERE {whereJsonPathMatches "@path"}"""


/// Functions for dealing with results
[<AutoOpen>]
module Results =
    
    /// Create a domain item from a document, specifying the field in which the document is found
    [<CompiledName "FromDocument">]
    let fromDocument<'T> field (row: RowReader) : 'T =
        Configuration.serializer().Deserialize<'T>(row.string field)
        
    /// Create a domain item from a document
    [<CompiledName "FromData">]
    let fromData<'T> row : 'T =
        fromDocument "data" row
    
    /// Extract a count from the column "it"
    [<CompiledName "ToCount">]
    let toCount (row: RowReader) =
        row.int "it"
    
    /// Extract a true/false value from the column "it"
    [<CompiledName "ToExists">]
    let toExists (row: RowReader) =
        row.bool "it"


/// Versions of queries that accept SqlProps as the last parameter
module WithProps =
    
    /// Commands to execute custom SQL queries
    [<RequireQualifiedAccess>]
    module Custom =

        /// Execute a query that returns a list of results
        [<CompiledName "FSharpList">]
        let list<'TDoc> query parameters (mapFunc: RowReader -> 'TDoc) sqlProps =
            Sql.query query sqlProps
            |> Sql.parameters parameters
            |> Sql.executeAsync mapFunc

        /// Execute a query that returns a list of results
        let List<'TDoc>(query, parameters, mapFunc: System.Func<RowReader, 'TDoc>, sqlProps) = backgroundTask {
            let! results = list<'TDoc> query (List.ofSeq parameters) mapFunc.Invoke sqlProps
            return ResizeArray results
        }
        
        /// Execute a query that returns one or no results; returns None if not found
        [<CompiledName "FSharpSingle">]
        let single<'TDoc> query parameters mapFunc sqlProps = backgroundTask {
            let! results = list<'TDoc> query parameters mapFunc sqlProps
            return FSharp.Collections.List.tryHead results
        }

        /// Execute a query that returns one or no results; returns null if not found
        let Single<'TDoc when 'TDoc: null>(
                query, parameters, mapFunc: System.Func<RowReader, 'TDoc>, sqlProps) = backgroundTask {
            let! result = single<'TDoc> query (FSharp.Collections.List.ofSeq parameters) mapFunc.Invoke sqlProps
            return Option.toObj result
        }

        /// Execute a query that returns no results
        [<CompiledName "NonQuery">]
        let nonQuery query parameters sqlProps =
            Sql.query query sqlProps
            |> Sql.parameters (FSharp.Collections.List.ofSeq parameters)
            |> Sql.executeNonQueryAsync
            |> ignoreTask
        
        /// Execute a query that returns a scalar value
        [<CompiledName "FSharpScalar">]
        let scalar<'T when 'T: struct> query parameters (mapFunc: RowReader -> 'T) sqlProps =
            Sql.query query sqlProps
            |> Sql.parameters parameters
            |> Sql.executeRowAsync mapFunc
        
        /// Execute a query that returns a scalar value
        let Scalar<'T when 'T: struct>(query, parameters, mapFunc: System.Func<RowReader, 'T>, sqlProps) =
            scalar<'T> query (FSharp.Collections.List.ofSeq parameters) mapFunc.Invoke sqlProps

    /// Table and index definition commands
    module Definition =
        
        /// Create a document table
        [<CompiledName "EnsureTable">]
        let ensureTable name sqlProps = backgroundTask {
            do! Custom.nonQuery (Query.Definition.ensureTable name) [] sqlProps
            do! Custom.nonQuery (Query.Definition.ensureKey   name) [] sqlProps
        }

        /// Create an index on documents in the specified table
        [<CompiledName "EnsureJsonIndex">]
        let ensureJsonIndex name idxType sqlProps =
            Custom.nonQuery (Query.Definition.ensureJsonIndex name idxType) [] sqlProps
        
        /// Create an index on field(s) within documents in the specified table
        [<CompiledName "EnsureFieldIndex">]
        let ensureFieldIndex tableName indexName fields sqlProps =
            Custom.nonQuery (Query.Definition.ensureIndexOn tableName indexName fields) [] sqlProps

    /// Commands to add documents
    [<AutoOpen>]
    module Document =
        
        /// Insert a new document
        [<CompiledName "Insert">]
        let insert<'TDoc> tableName (document: 'TDoc) sqlProps =
            Custom.nonQuery (Query.insert tableName) [ jsonParam "@data" document ] sqlProps

        /// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
        [<CompiledName "Save">]
        let save<'TDoc> tableName (document: 'TDoc) sqlProps =
            Custom.nonQuery (Query.save tableName) [ jsonParam "@data" document ] sqlProps

    /// Commands to count documents
    [<RequireQualifiedAccess>]
    module Count =
        
        /// Count all documents in a table
        [<CompiledName "All">]
        let all tableName sqlProps =
            Custom.scalar (Query.Count.all tableName) [] toCount sqlProps
        
        /// Count matching documents using a JSON field comparison (->> =)
        [<CompiledName "ByField">]
        let byField tableName fieldName op (value: obj) sqlProps =
            Custom.scalar (Query.Count.byField tableName fieldName op) [ fieldParam value ] toCount sqlProps
        
        /// Count matching documents using a JSON containment query (@>)
        [<CompiledName "ByContains">]
        let byContains tableName (criteria: 'TContains) sqlProps =
            Custom.scalar (Query.Count.byContains tableName) [ jsonParam "@criteria" criteria ] toCount sqlProps

        /// Count matching documents using a JSON Path match query (@?)
        [<CompiledName "ByJsonPath">]
        let byJsonPath tableName jsonPath sqlProps =
            Custom.scalar (Query.Count.byJsonPath tableName) [ "@path", Sql.string jsonPath ] toCount sqlProps
    
    /// Commands to determine if documents exist
    [<RequireQualifiedAccess>]
    module Exists =

        /// Determine if a document exists for the given ID
        [<CompiledName "ById">]
        let byId tableName (docId: 'TKey) sqlProps =
            Custom.scalar (Query.Exists.byId tableName) [ idParam docId ] toExists sqlProps

        /// Determine if a document exists using a JSON field comparison (->> =)
        [<CompiledName "ByField">]
        let byField tableName fieldName op (value: obj) sqlProps =
            Custom.scalar (Query.Exists.byField tableName fieldName op) [ fieldParam value ] toExists sqlProps
        
        /// Determine if a document exists using a JSON containment query (@>)
        [<CompiledName "ByContains">]
        let byContains tableName (criteria: 'TContains) sqlProps =
            Custom.scalar (Query.Exists.byContains tableName) [ jsonParam "@criteria" criteria ] toExists sqlProps

        /// Determine if a document exists using a JSON Path match query (@?)
        [<CompiledName "ByJsonPath">]
        let byJsonPath tableName jsonPath sqlProps =
            Custom.scalar (Query.Exists.byJsonPath tableName) [ "@path", Sql.string jsonPath ] toExists sqlProps

    /// Commands to determine if documents exist
    [<RequireQualifiedAccess>]
    module Find =
        
        /// Retrieve all documents in the given table
        [<CompiledName "FSharpAll">]
        let all<'TDoc> tableName sqlProps =
            Custom.list<'TDoc> (Query.selectFromTable tableName) [] fromData<'TDoc> sqlProps

        /// Retrieve all documents in the given table
        let All<'TDoc>(tableName, sqlProps) =
            Custom.List<'TDoc>(Query.selectFromTable tableName, [], fromData<'TDoc>, sqlProps)

        /// Retrieve a document by its ID (returns None if not found)
        [<CompiledName "FSharpById">]
        let byId<'TKey, 'TDoc> tableName (docId: 'TKey) sqlProps =
            Custom.single (Query.Find.byId tableName) [ idParam docId ] fromData<'TDoc> sqlProps

        /// Retrieve a document by its ID (returns null if not found)
        let ById<'TKey, 'TDoc when 'TDoc: null>(tableName, docId: 'TKey, sqlProps) =
            Custom.Single<'TDoc>(Query.Find.byId tableName, [ idParam docId ], fromData<'TDoc>, sqlProps)

        /// Retrieve documents matching a JSON field comparison (->> =)
        [<CompiledName "FSharpByField">]
        let byField<'TDoc> tableName fieldName op (value: obj) sqlProps =
            Custom.list<'TDoc> (Query.Find.byField tableName fieldName op) [ fieldParam value ] fromData<'TDoc> sqlProps
        
        /// Retrieve documents matching a JSON field comparison (->> =)
        let ByField<'TDoc>(tableName, fieldName, op, value: obj, sqlProps) =
            Custom.List<'TDoc>(
                Query.Find.byField tableName fieldName op, [ fieldParam value ], fromData<'TDoc>, sqlProps)
        
        /// Retrieve documents matching a JSON containment query (@>)
        [<CompiledName "FSharpByContains">]
        let byContains<'TDoc> tableName (criteria: obj) sqlProps =
            Custom.list<'TDoc>
                (Query.Find.byContains tableName) [ jsonParam "@criteria" criteria ] fromData<'TDoc> sqlProps

        /// Retrieve documents matching a JSON containment query (@>)
        let ByContains<'TDoc>(tableName, criteria: obj, sqlProps) =
            Custom.List<'TDoc>(
                Query.Find.byContains tableName, [ jsonParam "@criteria" criteria ], fromData<'TDoc>, sqlProps)

        /// Retrieve documents matching a JSON Path match query (@?)
        [<CompiledName "FSharpByJsonPath">]
        let byJsonPath<'TDoc> tableName jsonPath sqlProps =
            Custom.list<'TDoc>
                (Query.Find.byJsonPath tableName) [ "@path", Sql.string jsonPath ] fromData<'TDoc> sqlProps
        
        /// Retrieve documents matching a JSON Path match query (@?)
        let ByJsonPath<'TDoc>(tableName, jsonPath, sqlProps) =
            Custom.List<'TDoc>(
                Query.Find.byJsonPath tableName, [ "@path", Sql.string jsonPath ], fromData<'TDoc>, sqlProps)
        
        /// Retrieve the first document matching a JSON field comparison (->> =); returns None if not found
        [<CompiledName "FSharpFirstByField">]
        let firstByField<'TDoc> tableName fieldName op (value: obj) sqlProps =
            Custom.single<'TDoc>
                $"{Query.Find.byField tableName fieldName op} LIMIT 1" [ fieldParam value ] fromData<'TDoc> sqlProps
            
        /// Retrieve the first document matching a JSON field comparison (->> =); returns null if not found
        let FirstByField<'TDoc when 'TDoc: null>(tableName, fieldName, op, value: obj, sqlProps) =
            Custom.Single<'TDoc>(
                $"{Query.Find.byField tableName fieldName op} LIMIT 1", [ fieldParam value ], fromData<'TDoc>, sqlProps)
            
        /// Retrieve the first document matching a JSON containment query (@>); returns None if not found
        [<CompiledName "FSharpFirstByContains">]
        let firstByContains<'TDoc> tableName (criteria: obj) sqlProps =
            Custom.single<'TDoc>
                $"{Query.Find.byContains tableName} LIMIT 1" [ jsonParam "@criteria" criteria ] fromData<'TDoc> sqlProps

        /// Retrieve the first document matching a JSON containment query (@>); returns null if not found
        let FirstByContains<'TDoc when 'TDoc: null>(tableName, criteria: obj, sqlProps) =
            Custom.Single<'TDoc>(
                $"{Query.Find.byContains tableName} LIMIT 1",
                [ jsonParam "@criteria" criteria ],
                fromData<'TDoc>,
                sqlProps)

        /// Retrieve the first document matching a JSON Path match query (@?); returns None if not found
        [<CompiledName "FSharpFirstByJsonPath">]
        let firstByJsonPath<'TDoc> tableName jsonPath sqlProps =
            Custom.single<'TDoc>
                $"{Query.Find.byJsonPath tableName} LIMIT 1" [ "@path", Sql.string jsonPath ] fromData<'TDoc> sqlProps

        /// Retrieve the first document matching a JSON Path match query (@?); returns null if not found
        let FirstByJsonPath<'TDoc when 'TDoc: null>(tableName, jsonPath, sqlProps) =
            Custom.Single<'TDoc>(
                $"{Query.Find.byJsonPath tableName} LIMIT 1",
                [ "@path", Sql.string jsonPath ],
                fromData<'TDoc>,
                sqlProps)

    /// Commands to update documents
    [<RequireQualifiedAccess>]
    module Update =
        
        /// Update an entire document
        [<CompiledName "Full">]
        let full tableName (docId: 'TKey) (document: 'TDoc) sqlProps =
            Custom.nonQuery (Query.Update.full tableName) [ idParam docId; jsonParam "@data" document ] sqlProps
        
        /// Update an entire document
        [<CompiledName "FSharpFullFunc">]
        let fullFunc tableName (idFunc: 'TDoc -> 'TKey) (document: 'TDoc) sqlProps =
            full tableName (idFunc document) document sqlProps
        
        /// Update an entire document
        let FullFunc(tableName, idFunc: System.Func<'TDoc, 'TKey>, document: 'TDoc, sqlProps) =
            fullFunc tableName idFunc.Invoke document sqlProps
        
        /// Update a partial document
        [<CompiledName "PartialById">]
        let partialById tableName (docId: 'TKey) (partial: 'TPartial) sqlProps =
            Custom.nonQuery (Query.Update.partialById tableName) [ idParam docId; jsonParam "@data" partial ] sqlProps
        
        /// Update partial documents using a JSON field comparison query in the WHERE clause (->> =)
        [<CompiledName "PartialByField">]
        let partialByField tableName fieldName op (value: obj) (partial: 'TPartial) sqlProps =
            Custom.nonQuery
                (Query.Update.partialByField tableName fieldName op)
                [ jsonParam "@data" partial; fieldParam value ]
                sqlProps
        
        /// Update partial documents using a JSON containment query in the WHERE clause (@>)
        [<CompiledName "PartialByContains">]
        let partialByContains tableName (criteria: 'TContains) (partial: 'TPartial) sqlProps =
            Custom.nonQuery
                (Query.Update.partialByContains tableName)
                [ jsonParam "@data" partial; jsonParam "@criteria" criteria ]
                sqlProps
        
        /// Update partial documents using a JSON Path match query in the WHERE clause (@?)
        [<CompiledName "PartialByJsonPath">]
        let partialByJsonPath tableName jsonPath (partial: 'TPartial) sqlProps =
            Custom.nonQuery
                (Query.Update.partialByJsonPath tableName)
                [ jsonParam "@data" partial; "@path", Sql.string jsonPath ]
                sqlProps

    /// Commands to delete documents
    [<RequireQualifiedAccess>]
    module Delete =
        
        /// Delete a document by its ID
        [<CompiledName "ById">]
        let byId tableName (docId: 'TKey) sqlProps =
            Custom.nonQuery (Query.Delete.byId tableName) [ idParam docId ] sqlProps

        /// Delete documents by matching a JSON field comparison query (->> =)
        [<CompiledName "ByField">]
        let byField tableName fieldName op (value: obj) sqlProps =
            Custom.nonQuery (Query.Delete.byField tableName fieldName op) [ fieldParam value ] sqlProps
        
        /// Delete documents by matching a JSON contains query (@>)
        [<CompiledName "ByContains">]
        let byContains tableName (criteria: 'TCriteria) sqlProps =
            Custom.nonQuery (Query.Delete.byContains tableName) [ jsonParam "@criteria" criteria ] sqlProps

        /// Delete documents by matching a JSON Path match query (@?)
        [<CompiledName "ByJsonPath">]
        let byJsonPath tableName path sqlProps =
            Custom.nonQuery (Query.Delete.byJsonPath tableName) [ "@path", Sql.string path ] sqlProps
    
    
/// Commands to execute custom SQL queries
[<RequireQualifiedAccess>]
module Custom =

    /// Execute a query that returns a list of results
    [<CompiledName "FSharpList">]
    let list<'TDoc> query parameters (mapFunc: RowReader -> 'TDoc) =
        WithProps.Custom.list<'TDoc> query parameters mapFunc (fromDataSource ())

    /// Execute a query that returns a list of results
    let List<'TDoc>(query, parameters, mapFunc: System.Func<RowReader, 'TDoc>) =
        WithProps.Custom.List<'TDoc>(query, parameters, mapFunc, fromDataSource ())

    /// Execute a query that returns one or no results; returns None if not found
    [<CompiledName "FSharpSingle">]
    let single<'TDoc> query parameters (mapFunc: RowReader ->  'TDoc) =
        WithProps.Custom.single<'TDoc> query parameters mapFunc (fromDataSource ())

    /// Execute a query that returns one or no results; returns null if not found
    let Single<'TDoc when 'TDoc: null>(query, parameters, mapFunc: System.Func<RowReader, 'TDoc>) =
        WithProps.Custom.Single<'TDoc>(query, parameters, mapFunc, fromDataSource ())

    /// Execute a query that returns no results
    [<CompiledName "NonQuery">]
    let nonQuery query parameters =
        WithProps.Custom.nonQuery query parameters (fromDataSource ())

    /// Execute a query that returns a scalar value
    [<CompiledName "FSharpScalar">]
    let scalar<'T when 'T: struct> query parameters (mapFunc: RowReader -> 'T) =
        WithProps.Custom.scalar query parameters mapFunc (fromDataSource ())

    /// Execute a query that returns a scalar value
    let Scalar<'T when 'T: struct>(query, parameters, mapFunc: System.Func<RowReader, 'T>) =
        WithProps.Custom.Scalar<'T>(query, parameters, mapFunc, fromDataSource ())


/// Table and index definition commands
[<RequireQualifiedAccess>]
module Definition =
    
    /// Create a document table
    [<CompiledName "EnsureTable">]
    let ensureTable name =
        WithProps.Definition.ensureTable name (fromDataSource ())

    /// Create an index on documents in the specified table
    [<CompiledName "EnsureJsonIndex">]
    let ensureJsonIndex name idxType =
        WithProps.Definition.ensureJsonIndex name idxType (fromDataSource ())
    
    /// Create an index on field(s) within documents in the specified table
    [<CompiledName "EnsureFieldIndex">]
    let ensureFieldIndex tableName indexName fields =
        WithProps.Definition.ensureFieldIndex tableName indexName fields (fromDataSource ())


/// Document writing functions
[<AutoOpen>]
module Document =
    
    /// Insert a new document
    [<CompiledName "Insert">]
    let insert<'TDoc> tableName (document: 'TDoc) =
        WithProps.Document.insert<'TDoc> tableName document (fromDataSource ())

    /// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
    [<CompiledName "Save">]
    let save<'TDoc> tableName (document: 'TDoc) =
        WithProps.Document.save<'TDoc> tableName document (fromDataSource ())


/// Queries to count documents
[<RequireQualifiedAccess>]
module Count =
    
    /// Count all documents in a table
    [<CompiledName "All">]
    let all tableName =
        WithProps.Count.all tableName (fromDataSource ())
    
    /// Count matching documents using a JSON field comparison query (->> =)
    [<CompiledName "ByField">]
    let byField tableName fieldName op (value: obj) =
        WithProps.Count.byField tableName fieldName op value (fromDataSource ())
    
    /// Count matching documents using a JSON containment query (@>)
    [<CompiledName "ByContains">]
    let byContains tableName criteria =
        WithProps.Count.byContains tableName criteria (fromDataSource ())

    /// Count matching documents using a JSON Path match query (@?)
    [<CompiledName "ByJsonPath">]
    let byJsonPath tableName jsonPath =
        WithProps.Count.byJsonPath tableName jsonPath (fromDataSource ())


/// Queries to determine if documents exist
[<RequireQualifiedAccess>]
module Exists =

    /// Determine if a document exists for the given ID
    [<CompiledName "ById">]
    let byId tableName docId =
        WithProps.Exists.byId tableName docId (fromDataSource ())
    
    /// Determine if documents exist using a JSON field comparison query (->> =)
    [<CompiledName "ByField">]
    let byField tableName fieldName op (value: obj) =
        WithProps.Exists.byField tableName fieldName op value (fromDataSource ())
    
    /// Determine if documents exist using a JSON containment query (@>)
    [<CompiledName "ByContains">]
    let byContains tableName criteria =
        WithProps.Exists.byContains tableName criteria (fromDataSource ())

    /// Determine if documents exist using a JSON Path match query (@?)
    [<CompiledName "ByJsonPath">]
    let byJsonPath tableName jsonPath =
        WithProps.Exists.byJsonPath tableName jsonPath (fromDataSource ())


/// Commands to retrieve documents
[<RequireQualifiedAccess>]
module Find =
    
    /// Retrieve all documents in the given table
    [<CompiledName "FSharpAll">]
    let all<'TDoc> tableName =
        WithProps.Find.all<'TDoc> tableName (fromDataSource ())

    /// Retrieve all documents in the given table
    let All<'TDoc> tableName =
        WithProps.Find.All<'TDoc>(tableName, fromDataSource ())

    /// Retrieve a document by its ID; returns None if not found
    [<CompiledName "FSharpById">]
    let byId<'TKey, 'TDoc> tableName docId =
        WithProps.Find.byId<'TKey, 'TDoc> tableName docId (fromDataSource ())

    /// Retrieve a document by its ID; returns null if not found
    let ById<'TKey, 'TDoc when 'TDoc: null>(tableName, docId: 'TKey) =
        WithProps.Find.ById<'TKey, 'TDoc>(tableName, docId, fromDataSource ())

    /// Retrieve documents matching a JSON field comparison query (->> =)
    [<CompiledName "FSharpByField">]
    let byField<'TDoc> tableName fieldName op (value: obj) =
        WithProps.Find.byField<'TDoc> tableName fieldName op value (fromDataSource ())
    
    /// Retrieve documents matching a JSON field comparison query (->> =)
    let ByField<'TDoc>(tableName, fieldName, op, value: obj) =
        WithProps.Find.ByField<'TDoc>(tableName, fieldName, op, value, fromDataSource ())
    
    /// Retrieve documents matching a JSON containment query (@>)
    [<CompiledName "FSharpByContains">]
    let byContains<'TDoc> tableName (criteria: obj) =
        WithProps.Find.byContains<'TDoc> tableName criteria (fromDataSource ())

    /// Retrieve documents matching a JSON containment query (@>)
    let ByContains<'TDoc>(tableName, criteria: obj) =
        WithProps.Find.ByContains<'TDoc>(tableName, criteria, fromDataSource ())

    /// Retrieve documents matching a JSON Path match query (@?)
    [<CompiledName "FSharpByJsonPath">]
    let byJsonPath<'TDoc> tableName jsonPath =
        WithProps.Find.byJsonPath<'TDoc> tableName jsonPath (fromDataSource ())
    
    /// Retrieve documents matching a JSON Path match query (@?)
    let ByJsonPath<'TDoc>(tableName, jsonPath) =
        WithProps.Find.ByJsonPath<'TDoc>(tableName, jsonPath, fromDataSource ())
    
    /// Retrieve the first document matching a JSON field comparison query (->> =); returns None if not found
    [<CompiledName "FSharpFirstByField">]
    let firstByField<'TDoc> tableName fieldName op (value: obj) =
        WithProps.Find.firstByField<'TDoc> tableName fieldName op value (fromDataSource ())
    
    /// Retrieve the first document matching a JSON field comparison query (->> =); returns null if not found
    let FirstByField<'TDoc when 'TDoc: null>(tableName, fieldName, op, value: obj) =
        WithProps.Find.FirstByField<'TDoc>(tableName, fieldName, op, value, fromDataSource ())
    
    /// Retrieve the first document matching a JSON containment query (@>); returns None if not found
    [<CompiledName "FSharpFirstByContains">]
    let firstByContains<'TDoc> tableName (criteria: obj) =
        WithProps.Find.firstByContains<'TDoc> tableName criteria (fromDataSource ())

    /// Retrieve the first document matching a JSON containment query (@>); returns null if not found
    let FirstByContains<'TDoc when 'TDoc: null>(tableName, criteria: obj) =
        WithProps.Find.FirstByContains<'TDoc>(tableName, criteria, fromDataSource ())

    /// Retrieve the first document matching a JSON Path match query (@?); returns None if not found
    [<CompiledName "FSharpFirstByJsonPath">]
    let firstByJsonPath<'TDoc> tableName jsonPath =
        WithProps.Find.firstByJsonPath<'TDoc> tableName jsonPath (fromDataSource ())

    /// Retrieve the first document matching a JSON Path match query (@?); returns null if not found
    let FirstByJsonPath<'TDoc when 'TDoc: null>(tableName, jsonPath) =
        WithProps.Find.FirstByJsonPath<'TDoc>(tableName, jsonPath, fromDataSource ())


/// Commands to update documents
[<RequireQualifiedAccess>]
module Update =

    /// Update a full document
    [<CompiledName "Full">]
    let full tableName (docId: 'TKey) (document: 'TDoc) =
        WithProps.Update.full tableName docId document (fromDataSource ())

    /// Update a full document
    [<CompiledName "FSharpFullFunc">]
    let fullFunc tableName (idFunc: 'TDoc -> 'TKey) (document: 'TDoc) =
        WithProps.Update.fullFunc tableName idFunc document (fromDataSource ())

    /// Update a full document
    let FullFunc(tableName, idFunc: System.Func<'TDoc, 'TKey>, document: 'TDoc) =
        WithProps.Update.FullFunc(tableName, idFunc, document, fromDataSource ())

    /// Update a partial document
    [<CompiledName "PartialById">]
    let partialById tableName (docId: 'TKey) (partial: 'TPartial) =
        WithProps.Update.partialById tableName docId partial (fromDataSource ())
    
    /// Update partial documents using a JSON field comparison query in the WHERE clause (->> =)
    [<CompiledName "PartialByField">]
    let partialByField tableName fieldName op (value: obj) (partial: 'TPartial) =
        WithProps.Update.partialByField tableName fieldName op value partial (fromDataSource ())
    
    /// Update partial documents using a JSON containment query in the WHERE clause (@>)
    [<CompiledName "PartialByContains">]
    let partialByContains tableName (criteria: 'TCriteria) (partial: 'TPartial) =
        WithProps.Update.partialByContains tableName criteria partial (fromDataSource ())
    
    /// Update partial documents using a JSON Path match query in the WHERE clause (@?)
    [<CompiledName "PartialByJsonPath">]
    let partialByJsonPath tableName jsonPath (partial: 'TPartial) =
        WithProps.Update.partialByJsonPath tableName jsonPath partial (fromDataSource ())


/// Commands to delete documents
[<RequireQualifiedAccess>]
module Delete =
    
    /// Delete a document by its ID
    [<CompiledName "ById">]
    let byId tableName (docId: 'TKey) =
        WithProps.Delete.byId tableName docId (fromDataSource ())

    /// Delete documents by matching a JSON field comparison query (->> =)
    [<CompiledName "ByField">]
    let byField tableName fieldName op (value: obj) =
        WithProps.Delete.byField tableName fieldName op value (fromDataSource ())
    
    /// Delete documents by matching a JSON containment query (@>)
    [<CompiledName "ByContains">]
    let byContains tableName (criteria: 'TContains) =
        WithProps.Delete.byContains tableName criteria (fromDataSource ())

    /// Delete documents by matching a JSON Path match query (@?)
    [<CompiledName "ByJsonPath">]
    let byJsonPath tableName path =
        WithProps.Delete.byJsonPath tableName path (fromDataSource ())
