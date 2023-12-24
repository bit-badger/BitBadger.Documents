module BitBadger.Documents.Sqlite

open BitBadger.Documents
open Microsoft.Data.Sqlite

/// Configuration for document handling
module Configuration =

    /// The connection string to use for query execution
    let mutable internal connectionString: string option = None

    /// Register a connection string to use for query execution (enables foreign keys)
    [<CompiledName "UseConnectionString">]
    let useConnectionString connStr =
        let builder = SqliteConnectionStringBuilder(connStr)
        builder.ForeignKeys <- Option.toNullable (Some true)
        connectionString <- Some (string builder)
    
    /// Retrieve the currently configured data source
    [<CompiledName "DbConn">]
    let dbConn () =
        match connectionString with
        | Some connStr ->
            let conn = new SqliteConnection(connStr)
            conn.Open()
            conn
        | None -> invalidOp "Please provide a connection string before attempting data access"


[<RequireQualifiedAccess>]
module Query =
    
    /// Data definition
    module Definition =

        /// SQL statement to create a document table
        [<CompiledName "EnsureTable">]
        let ensureTable name =
            Query.Definition.ensureTableFor name "TEXT"


/// Create an ID parameter (key will be treated as a string)
[<CompiledName "IdParam">]
let idParam (key: 'TKey) =
    SqliteParameter("@id", string key)

/// Create a parameter with a JSON value
[<CompiledName "JsonParam">]
let jsonParam name (it: 'TJson) =
    SqliteParameter(name, Configuration.serializer().Serialize it)

/// Create a JSON field parameter
[<CompiledName "FieldParam">]
let fieldParam (value: obj) =
    SqliteParameter("@field", value)

/// Create a domain item from a document, specifying the field in which the document is found
[<CompiledName "FromDocument">]
let fromDocument<'TDoc> field (rdr: SqliteDataReader) : 'TDoc =
    Configuration.serializer().Deserialize<'TDoc>(rdr.GetString(rdr.GetOrdinal(field)))

/// Create a domain item from a document
[<CompiledName "FromData">]
let fromData<'TDoc> rdr =
    fromDocument<'TDoc> "data" rdr

/// Create a list of items for the results of the given command, using the specified mapping function
[<CompiledName "ToCustomList">]
let toCustomList<'TDoc> (cmd: SqliteCommand) (mapFunc: SqliteDataReader -> 'TDoc) = backgroundTask {
    use! rdr = cmd.ExecuteReaderAsync()
    let mutable it = Seq.empty<'TDoc>
    while! rdr.ReadAsync() do
        it <- Seq.append it (Seq.singleton (mapFunc rdr))
    return List.ofSeq it
}

/// Create a list of items for the results of the given command
[<CompiledName "ToDocumentList">]
let toDocumentList<'TDoc> (cmd: SqliteCommand) =
    toCustomList<'TDoc> cmd fromData

/// Execute a non-query command
let internal write (cmd: SqliteCommand) = backgroundTask {
    let! _ = cmd.ExecuteNonQueryAsync()
    ()
}


/// Command creation helper functions
[<AutoOpen>]
module private Helpers =
    
    let addParam (cmd: SqliteCommand) it =
        cmd.Parameters.Add it |> ignore
    
    /// Add an ID parameter to a command
    let addIdParam (cmd: SqliteCommand) (key: 'TKey) =
        addParam cmd (idParam key)

    /// Add a JSON document parameter to a command
    let addJsonParam (cmd: SqliteCommand) name (it: 'TJson) =
        addParam cmd (jsonParam name it)

    /// Add ID (@id) and document (@data) parameters to a command
    let addIdAndDocParams cmd (docId: 'TKey) (doc: 'TDoc) =
        addIdParam cmd docId
        addJsonParam cmd "@data" doc

    /// Add a parameter to a SQLite command, ignoring the return value (can still be accessed on cmd via indexing)
    let addFieldParam (cmd: SqliteCommand) (value: obj) =
        addParam cmd (SqliteParameter("@field", value))
    
    /// Execute a non-query statement to manipulate a document
    let executeNonQuery query (document: 'T) (conn: SqliteConnection) =
        use cmd = conn.CreateCommand()
        cmd.CommandText <- query
        addJsonParam cmd "@data" document
        write cmd

    /// Execute a non-query statement to manipulate a document with an ID specified
    let executeNonQueryWithId query (docId: 'TKey) (document: 'TDoc) (conn: SqliteConnection) =
        use cmd = conn.CreateCommand()
        cmd.CommandText <- query
        addIdAndDocParams cmd docId document
        write cmd


/// Versions of queries that accept a SqliteConnection as the last parameter
module WithConn =
    
    /// Commands to execute custom SQL queries
    [<RequireQualifiedAccess>]
    module Custom =

        /// Execute a query that returns a list of results
        [<CompiledName "FSharpList">]
        let list<'TDoc> query (parameters: SqliteParameter seq) (mapFunc: SqliteDataReader -> 'TDoc)
                (conn: SqliteConnection) =
            use cmd = conn.CreateCommand()
            cmd.CommandText <- query
            cmd.Parameters.AddRange parameters
            toCustomList<'TDoc> cmd mapFunc
        
        /// Execute a query that returns a list of results
        let List<'TDoc>(query, parameters, mapFunc: System.Func<SqliteDataReader, 'TDoc>, conn) = backgroundTask {
            let! results = list<'TDoc> query parameters mapFunc.Invoke conn
            return ResizeArray<'TDoc> results
        }
        
        /// Execute a query that returns one or no results (returns None if not found)
        [<CompiledName "FSharpSingle">]
        let single<'TDoc> query parameters (mapFunc: SqliteDataReader -> 'TDoc) conn = backgroundTask {
            let! results = list query parameters mapFunc conn
            return FSharp.Collections.List.tryHead results
        }
        
        /// Execute a query that returns one or no results (returns null if not found)
        let Single<'TDoc when 'TDoc: null>(
                query, parameters, mapFunc: System.Func<SqliteDataReader, 'TDoc>, conn
            ) = backgroundTask {
            let! result = single<'TDoc> query parameters mapFunc.Invoke conn
            return Option.toObj result
        }
        
        /// Execute a query that does not return a value
        [<CompiledName "NonQuery">]
        let nonQuery query (parameters: SqliteParameter seq) (conn: SqliteConnection) =
            use cmd = conn.CreateCommand()
            cmd.CommandText <- query
            cmd.Parameters.AddRange parameters
            write cmd

        /// Execute a query that returns a scalar value
        [<CompiledName "FSharpScalar">]
        let scalar<'T when 'T : struct> query (parameters: SqliteParameter seq) (mapFunc: SqliteDataReader -> 'T)
                (conn: SqliteConnection) = backgroundTask {
            use cmd = conn.CreateCommand()
            cmd.CommandText <- query
            cmd.Parameters.AddRange parameters
            use! rdr = cmd.ExecuteReaderAsync()
            let! isFound = rdr.ReadAsync()
            return if isFound then mapFunc rdr else Unchecked.defaultof<'T>
        }

        /// Execute a query that returns a scalar value
        let Scalar<'T when 'T: struct>(query, parameters, mapFunc: System.Func<SqliteDataReader, 'T>, conn) =
            scalar<'T> query parameters mapFunc.Invoke conn

    /// Functions to create tables and indexes
    [<RequireQualifiedAccess>]
    module Definition =
        
        /// Create a document table
        [<CompiledName "EnsureTable">]
        let ensureTable name conn = backgroundTask {
            do! Custom.nonQuery (Query.Definition.ensureTable name) [] conn
            do! Custom.nonQuery (Query.Definition.ensureKey name)   [] conn
        }
        
        /// Create an index on a document table
        [<CompiledName "EnsureIndex">]
        let ensureIndex tableName indexName fields conn =
            Custom.nonQuery (Query.Definition.ensureIndexOn tableName indexName fields) [] conn

    /// Insert a new document
    [<CompiledName "Insert">]
    let insert<'TDoc> tableName (document: 'TDoc) conn =
        Custom.nonQuery (Query.insert tableName) [ jsonParam "@data" document ] conn

    /// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
    [<CompiledName "Save">]
    let save<'TDoc> tableName (document: 'TDoc) conn =
        Custom.nonQuery (Query.save tableName) [ jsonParam "@data" document ] conn

    /// Commands to count documents
    [<RequireQualifiedAccess>]
    module Count =
        
        /// Count all documents in a table
        [<CompiledName "All">]
        let all tableName conn =
            Custom.scalar (Query.Count.all tableName) [] (_.GetInt64(0)) conn
        
        /// Count matching documents using a comparison on a JSON field
        [<CompiledName "ByField">]
        let byField tableName fieldName op (value: obj) conn =
            Custom.scalar (Query.Count.byField tableName fieldName op) [  fieldParam value ] (_.GetInt64(0)) conn

    /// Commands to determine if documents exist
    [<RequireQualifiedAccess>]
    module Exists =
        
        /// SQLite returns a 0 for not-exists and 1 for exists
        let private exists (rdr: SqliteDataReader) =
            rdr.GetInt64(0) > 0
        
        /// Determine if a document exists for the given ID
        [<CompiledName "ById">]
        let byId tableName (docId: 'TKey) conn =
            Custom.scalar (Query.Exists.byId tableName) [ idParam docId ] exists conn

        /// Determine if a document exists using a comparison on a JSON field
        [<CompiledName "ByField">]
        let byField tableName fieldName op (value: obj) conn =
            Custom.scalar (Query.Exists.byField tableName fieldName op) [ fieldParam value ] exists conn
    
    /// Commands to retrieve documents
    [<RequireQualifiedAccess>]
    module Find =
        
        /// Retrieve all documents in the given table
        [<CompiledName "FSharpAll">]
        let all<'TDoc> tableName conn =
            Custom.list<'TDoc> (Query.selectFromTable tableName) [] fromData<'TDoc> conn

        /// Retrieve all documents in the given table
        let All<'TDoc>(tableName, conn) =
            Custom.List(Query.selectFromTable tableName, [], fromData<'TDoc>, conn)

        /// Retrieve a document by its ID (returns None if not found)
        [<CompiledName "FSharpById">]
        let byId<'TKey, 'TDoc> tableName (docId: 'TKey) conn =
            Custom.single<'TDoc> (Query.Find.byId tableName) [ idParam docId ] fromData<'TDoc> conn

        /// Retrieve a document by its ID (returns null if not found)
        let ById<'TKey, 'TDoc when 'TDoc: null>(tableName, docId: 'TKey, conn) =
            Custom.Single<'TDoc>(Query.Find.byId tableName, [ idParam docId ], fromData<'TDoc>, conn)

        /// Retrieve documents via a comparison on a JSON field
        [<CompiledName "FSharpByField">]
        let byField<'TDoc> tableName fieldName op (value: obj) conn =
            Custom.list<'TDoc> (Query.Find.byField tableName fieldName op) [ fieldParam value ] fromData<'TDoc> conn 
        
        /// Retrieve documents via a comparison on a JSON field
        let ByField<'TDoc>(tableName, fieldName, op, value: obj, conn) =
            Custom.List<'TDoc>(Query.Find.byField tableName fieldName op, [ fieldParam value ], fromData<'TDoc>, conn) 
        
        /// Retrieve documents via a comparison on a JSON field, returning only the first result
        [<CompiledName "FSharpFirstByField">]
        let firstByField<'TDoc> tableName fieldName op (value: obj) conn =
            Custom.single
                $"{Query.Find.byField tableName fieldName op} LIMIT 1" [ fieldParam value ] fromData<'TDoc> conn
        
        /// Retrieve documents via a comparison on a JSON field, returning only the first result
        let FirstByField<'TDoc when 'TDoc: null>(tableName, fieldName, op, value: obj, conn) =
            Custom.Single(
                $"{Query.Find.byField tableName fieldName op} LIMIT 1", [ fieldParam value ], fromData<'TDoc>, conn)
    
    /// Commands to update documents
    [<RequireQualifiedAccess>]
    module Update =
        
        /// Update an entire document
        [<CompiledName "Full">]
        let full tableName (docId: 'TKey) (document: 'TDoc) conn =
            Custom.nonQuery (Query.Update.full tableName) [ idParam docId; jsonParam "@data" document ] conn
        
        /// Update an entire document
        [<CompiledName "FSharpFullFunc">]
        let fullFunc tableName (idFunc: 'TDoc -> 'TKey) (document: 'TDoc) conn =
            full tableName (idFunc document) document conn
        
        /// Update an entire document
        let FullFunc(tableName, idFunc: System.Func<'TDoc, 'TKey>, document: 'TDoc, conn) =
            fullFunc tableName idFunc.Invoke document conn
        
        /// Update a partial document
        [<CompiledName "PartialById">]
        let partialById tableName (docId: 'TKey) (partial: 'TPatch) conn =
            Custom.nonQuery (Query.Update.partialById tableName) [ idParam docId; jsonParam "@data" partial ] conn
        
        /// Update partial documents using a comparison on a JSON field
        [<CompiledName "PartialByField">]
        let partialByField tableName fieldName op (value: obj) (partial: 'TPatch) (conn: SqliteConnection) =
            Custom.nonQuery
                (Query.Update.partialByField tableName fieldName op)
                [ fieldParam value; jsonParam "@data" partial ]
                conn

    /// Commands to delete documents
    [<RequireQualifiedAccess>]
    module Delete =
        
        /// Delete a document by its ID
        [<CompiledName "ById">]
        let byId tableName (docId: 'TKey) conn =
            Custom.nonQuery (Query.Delete.byId tableName) [ idParam docId ] conn

        /// Delete documents by matching a comparison on a JSON field
        [<CompiledName "ByField">]
        let byField tableName fieldName op (value: obj) conn =
            Custom.nonQuery (Query.Delete.byField tableName fieldName op) [ fieldParam value ] conn

/// Functions to create tables and indexes
[<RequireQualifiedAccess>]
module Definition =

    /// Create a document table
    let ensureTable name =
        use conn = Configuration.dbConn ()
        WithConn.Definition.ensureTable name conn

/// Insert a new document
let insert<'TDoc> tableName (document: 'TDoc) =
    use conn = Configuration.dbConn ()
    WithConn.insert tableName document conn

/// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
let save<'TDoc> tableName (document: 'TDoc) =
    use conn = Configuration.dbConn ()
    WithConn.save tableName document conn

/// Commands to count documents
[<RequireQualifiedAccess>]
module Count =
    
    /// Count all documents in a table
    let all tableName =
        use conn = Configuration.dbConn ()
        WithConn.Count.all tableName conn
    
    /// Count matching documents using a comparison on a JSON field
    let byField tableName fieldName op (value: obj) =
        use conn = Configuration.dbConn ()
        WithConn.Count.byField tableName fieldName op value conn

/// Commands to determine if documents exist
[<RequireQualifiedAccess>]
module Exists =

    /// Determine if a document exists for the given ID
    let byId tableName (docId: 'TKey) =
        use conn = Configuration.dbConn ()
        WithConn.Exists.byId tableName docId conn

    /// Determine if a document exists using a comparison on a JSON field
    let byField tableName fieldName op (value: obj) =
        use conn = Configuration.dbConn ()
        WithConn.Exists.byField tableName fieldName op value conn

/// Commands to determine if documents exist
[<RequireQualifiedAccess>]
module Find =
    
    /// Retrieve all documents in the given table
    let all<'TDoc> tableName =
        use conn = Configuration.dbConn ()
        WithConn.Find.all<'TDoc> tableName conn

    /// Retrieve a document by its ID
    let byId<'TKey, 'TDoc> tableName docId =
        use conn = Configuration.dbConn ()
        WithConn.Find.byId<'TKey, 'TDoc> tableName docId conn

    /// Retrieve documents via a comparison on a JSON field
    let byField<'TDoc> tableName fieldName op value =
        use conn = Configuration.dbConn ()
        WithConn.Find.byField<'TDoc> tableName fieldName op value conn

    /// Retrieve documents via a comparison on a JSON field, returning only the first result
    let firstByField<'TDoc> tableName fieldName op value =
        use conn = Configuration.dbConn ()
        WithConn.Find.firstByField<'TDoc> tableName fieldName op value conn

/// Commands to update documents
[<RequireQualifiedAccess>]
module Update =
    
    /// Update an entire document
    let full tableName (docId: 'TKey) (document: 'TDoc) =
        use conn = Configuration.dbConn ()
        WithConn.Update.full tableName docId document conn
    
    /// Update an entire document
    let fullFunc tableName (idFunc: 'TDoc -> 'TKey) (document: 'TDoc) =
        use conn = Configuration.dbConn ()
        WithConn.Update.fullFunc tableName idFunc document conn
    
    /// Update a partial document
    let partialById tableName (docId: 'TKey) (partial: 'TPatch) =
        use conn = Configuration.dbConn ()
        WithConn.Update.partialById tableName docId partial conn
    
    /// Update partial documents using a comparison on a JSON field in the WHERE clause
    let partialByField tableName fieldName op (value: obj) (partial: 'TPatch) =
        use conn = Configuration.dbConn ()
        WithConn.Update.partialByField tableName fieldName op value partial conn

/// Commands to delete documents
[<RequireQualifiedAccess>]
module Delete =
    
    /// Delete a document by its ID
    let byId tableName (docId: 'TKey) =
        use conn = Configuration.dbConn ()
        WithConn.Delete.byId tableName docId conn

    /// Delete documents by matching a comparison on a JSON field
    let byField tableName fieldName op (value: obj) =
        use conn = Configuration.dbConn ()
        WithConn.Delete.byField tableName fieldName op value conn

/// Commands to execute custom SQL queries
[<RequireQualifiedAccess>]
module Custom =

    /// Execute a query that returns a list of results
    let list<'TDoc> query parameters (mapFunc: SqliteDataReader -> 'TDoc) =
        use conn = Configuration.dbConn ()
        WithConn.Custom.list<'TDoc> query parameters mapFunc conn

    /// Execute a query that returns one or no results
    let single<'TDoc> query parameters (mapFunc: SqliteDataReader -> 'TDoc) =
        use conn = Configuration.dbConn ()
        WithConn.Custom.single<'TDoc> query parameters mapFunc conn

    /// Execute a query that does not return a value
    let nonQuery query parameters =
        use conn = Configuration.dbConn ()
        WithConn.Custom.nonQuery query parameters conn
    
    /// Execute a query that returns a scalar value
    let scalar<'T when 'T : struct> query parameters (mapFunc: SqliteDataReader -> 'T) =
        use conn = Configuration.dbConn ()
        WithConn.Custom.scalar<'T> query parameters mapFunc conn

[<AutoOpen>]
module Extensions =

    type SqliteConnection with
        
        /// Create a document table
        member conn.ensureTable name =
            WithConn.Definition.ensureTable name conn
        
        /// Create an index on a document table
        member conn.ensureIndex tableName indexName fields =
            WithConn.Definition.ensureIndex tableName indexName fields conn
        
        /// Insert a new document
        member conn.insert<'TDoc> tableName (document: 'TDoc) =
            WithConn.insert<'TDoc> tableName document conn

        /// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
        member conn.save<'TDoc> tableName (document: 'TDoc) =
            WithConn.save tableName document conn

        /// Count all documents in a table
        member conn.countAll tableName =
            WithConn.Count.all tableName conn
        
        /// Count matching documents using a comparison on a JSON field
        member conn.countByField tableName fieldName op (value: obj) =
            WithConn.Count.byField tableName fieldName op value conn
        
        /// Determine if a document exists for the given ID
        member conn.existsById tableName (docId: 'TKey) =
            WithConn.Exists.byId tableName docId conn

        /// Determine if a document exists using a comparison on a JSON field
        member conn.existsByField tableName fieldName op (value: obj) =
            WithConn.Exists.byField tableName fieldName op value conn

        /// Retrieve all documents in the given table
        member conn.findAll<'TDoc> tableName =
            WithConn.Find.all<'TDoc> tableName conn

        /// Retrieve a document by its ID
        member conn.findById<'TKey, 'TDoc> tableName (docId: 'TKey) =
            WithConn.Find.byId<'TKey, 'TDoc> tableName docId conn

        /// Retrieve documents via a comparison on a JSON field
        member conn.findByField<'TDoc> tableName fieldName op (value: obj) =
            WithConn.Find.byField<'TDoc> tableName fieldName op value conn

        /// Retrieve documents via a comparison on a JSON field, returning only the first result
        member conn.findFirstByField<'TDoc> tableName fieldName op (value: obj) =
            WithConn.Find.firstByField<'TDoc> tableName fieldName op value conn

        /// Update an entire document
        member conn.updateFull tableName (docId: 'TKey) (document: 'TDoc) =
            WithConn.Update.full tableName docId document conn
        
        /// Update an entire document
        member conn.updateFullFunc tableName (idFunc: 'TDoc -> 'TKey) (document: 'TDoc) =
            WithConn.Update.fullFunc tableName idFunc document conn
        
        /// Update a partial document
        member conn.updatePartialById tableName (docId: 'TKey) (partial: 'TPatch) =
            WithConn.Update.partialById tableName docId partial conn
        
        /// Update partial documents using a comparison on a JSON field
        member conn.updatePartialByField tableName fieldName op (value: obj) (partial: 'TPatch) =
            WithConn.Update.partialByField tableName fieldName op value partial conn

        /// Delete a document by its ID
        member conn.deleteById tableName (docId: 'TKey) =
            WithConn.Delete.byId tableName docId conn

        /// Delete documents by matching a comparison on a JSON field
        member conn.deleteByField tableName fieldName op (value: obj) =
            WithConn.Delete.byField tableName fieldName op value conn

        /// Execute a query that returns a list of results
        member conn.customList<'TDoc> query parameters mapFunc =
            WithConn.Custom.list<'TDoc> query parameters mapFunc conn

        /// Execute a query that returns one or no results
        member conn.customSingle<'TDoc> query parameters mapFunc =
            WithConn.Custom.single<'TDoc> query parameters mapFunc conn
        
        /// Execute a query that does not return a value
        member conn.customNonQuery query parameters =
            WithConn.Custom.nonQuery query parameters conn

        /// Execute a query that returns a scalar value
        member conn.customScalar<'T when 'T: struct> query parameters mapFunc =
            WithConn.Custom.scalar<'T> query parameters mapFunc conn
