namespace BitBadger.Documents.Sqlite

open BitBadger.Documents
open Microsoft.Data.Sqlite

/// Configuration for document handling
module Configuration =

    /// The connection string to use for query execution
    let mutable internal connectionString: string option = None

    /// Register a connection string to use for query execution (enables foreign keys)
    [<CompiledName "UseConnectionString">]
    let useConnectionString connStr =
        let builder = SqliteConnectionStringBuilder connStr
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


/// Query definitions
[<RequireQualifiedAccess>]
module Query =
    
    /// Data definition
    module Definition =

        /// SQL statement to create a document table
        [<CompiledName "EnsureTable">]
        let ensureTable name =
            Query.Definition.ensureTableFor name "TEXT"
    
    /// Document patching (partial update) queries
    module Patch =
        
        /// Create an UPDATE statement to patch documents
        let internal update tableName whereClause =
            $"UPDATE %s{tableName} SET data = json_patch(data, json(@data)) WHERE %s{whereClause}"
        
        /// Query to patch (partially update) a document by its ID
        [<CompiledName "ById">]
        let byId tableName =
            Query.whereById "@id" |> update tableName
            
        /// Query to patch (partially update) a document via a comparison on a JSON field
        [<CompiledName "ByField">]
        let byField tableName field =
            Query.whereByField field "@field" |> update tableName
    
    /// Queries to remove fields from documents
    module RemoveFields =
        
        /// Create an UPDATE statement to remove parameters
        let internal update tableName (parameters: SqliteParameter list) whereClause =
            let paramNames = parameters |> List.map _.ParameterName |> String.concat ", "
            $"UPDATE %s{tableName} SET data = json_remove(data, {paramNames}) WHERE {whereClause}"
            
        /// Query to remove fields from a document by the document's ID
        [<CompiledName "FSharpById">]
        let byId tableName parameters =
            Query.whereById "@id" |> update tableName parameters
        
        /// Query to remove fields from a document by the document's ID
        let ById(tableName, parameters) =
            byId tableName (List.ofSeq parameters)
        
        /// Query to remove fields from documents via a comparison on a JSON field within the document
        [<CompiledName "FSharpByField">]
        let byField tableName field parameters =
            Query.whereByField field "@field" |> update tableName parameters
        
        /// Query to remove fields from documents via a comparison on a JSON field within the document
        let ByField(tableName, field, parameters) =
            byField tableName field (List.ofSeq parameters)


/// Parameter handling helpers
[<AutoOpen>]
module Parameters =
    
    /// Create an ID parameter (name "@id", key will be treated as a string)
    [<CompiledName "Id">]
    let idParam (key: 'TKey) =
        SqliteParameter("@id", string key)

    /// Create a parameter with a JSON value
    [<CompiledName "Json">]
    let jsonParam name (it: 'TJson) =
        SqliteParameter(name, Configuration.serializer().Serialize it)

    /// Create a JSON field parameter (name "@field")
    [<CompiledName "FSharpAddField">]
    let addFieldParam name field parameters =
        match field.Op with EX | NEX -> parameters | _ -> SqliteParameter(name, field.Value) :: parameters
    
    /// Create a JSON field parameter (name "@field")
    let AddField(name, field, parameters) =
        match field.Op with
        | EX | NEX -> parameters
        | _ -> SqliteParameter(name, field.Value) |> Seq.singleton |> Seq.append parameters

    /// Append JSON field name parameters for the given field names to the given parameters
    [<CompiledName "FSharpFieldNames">]
    let fieldNameParams paramName (fieldNames: string list) =
        fieldNames |> List.mapi (fun idx name -> SqliteParameter($"%s{paramName}{idx}", $"$.{name}"))

    /// Append JSON field name parameters for the given field names to the given parameters
    let FieldNames(paramName, fieldNames: string seq) =
        fieldNames |> Seq.mapi (fun idx name -> SqliteParameter($"%s{paramName}{idx}", $"$.{name}"))

    /// An empty parameter sequence
    [<CompiledName "None">]
    let noParams =
        Seq.empty<SqliteParameter>


/// Helper functions for handling results
[<AutoOpen>]
module Results =
    
    /// Create a domain item from a document, specifying the field in which the document is found
    [<CompiledName "FromDocument">]
    let fromDocument<'TDoc> field (rdr: SqliteDataReader) : 'TDoc =
        Configuration.serializer().Deserialize<'TDoc>(rdr.GetString(rdr.GetOrdinal(field)))

    /// Create a domain item from a document
    [<CompiledName "FromData">]
    let fromData<'TDoc> rdr =
        fromDocument<'TDoc> "data" rdr

    /// Create a list of items for the results of the given command, using the specified mapping function
    [<CompiledName "FSharpToCustomList">]
    let toCustomList<'TDoc> (cmd: SqliteCommand) (mapFunc: SqliteDataReader -> 'TDoc) = backgroundTask {
        use! rdr = cmd.ExecuteReaderAsync()
        let mutable it = Seq.empty<'TDoc>
        while! rdr.ReadAsync() do
            it <- Seq.append it (Seq.singleton (mapFunc rdr))
        return List.ofSeq it
    }
    
    /// Extract a count from the first column
    [<CompiledName "ToCount">]
    let toCount (row: SqliteDataReader) =
        row.GetInt64 0
    
    /// Extract a true/false value from a count in the first column
    [<CompiledName "ToExists">]
    let toExists row =
        toCount(row) > 0L


[<AutoOpen>]
module internal Helpers =
    
    /// Execute a non-query command
    let internal write (cmd: SqliteCommand) = backgroundTask {
        let! _ = cmd.ExecuteNonQueryAsync()
        ()
    }


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
        [<CompiledName "EnsureFieldIndex">]
        let ensureFieldIndex tableName indexName fields conn =
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
            Custom.scalar (Query.Count.all tableName) [] toCount conn
        
        /// Count matching documents using a comparison on a JSON field
        [<CompiledName "ByField">]
        let byField tableName field conn =
            Custom.scalar (Query.Count.byField tableName field) (addFieldParam "@field" field []) toCount conn

    /// Commands to determine if documents exist
    [<RequireQualifiedAccess>]
    module Exists =
        
        /// Determine if a document exists for the given ID
        [<CompiledName "ById">]
        let byId tableName (docId: 'TKey) conn =
            Custom.scalar (Query.Exists.byId tableName) [ idParam docId ] toExists conn

        /// Determine if a document exists using a comparison on a JSON field
        [<CompiledName "ByField">]
        let byField tableName field conn =
            Custom.scalar (Query.Exists.byField tableName field) (addFieldParam "@field" field []) toExists conn
    
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
        let byField<'TDoc> tableName field conn =
            Custom.list<'TDoc>
                (Query.Find.byField tableName field) (addFieldParam "@field" field []) fromData<'TDoc> conn 
        
        /// Retrieve documents via a comparison on a JSON field
        let ByField<'TDoc>(tableName, field, conn) =
            Custom.List<'TDoc>(
                Query.Find.byField tableName field, addFieldParam "@field" field [], fromData<'TDoc>, conn) 
        
        /// Retrieve documents via a comparison on a JSON field, returning only the first result
        [<CompiledName "FSharpFirstByField">]
        let firstByField<'TDoc> tableName field conn =
            Custom.single
                $"{Query.Find.byField tableName field} LIMIT 1" (addFieldParam "@field" field []) fromData<'TDoc> conn
        
        /// Retrieve documents via a comparison on a JSON field, returning only the first result
        let FirstByField<'TDoc when 'TDoc: null>(tableName, field, conn) =
            Custom.Single(
                $"{Query.Find.byField tableName field} LIMIT 1", addFieldParam "@field" field [], fromData<'TDoc>, conn)
    
    /// Commands to update documents
    [<RequireQualifiedAccess>]
    module Update =
        
        /// Update an entire document by its ID
        [<CompiledName "ById">]
        let byId tableName (docId: 'TKey) (document: 'TDoc) conn =
            Custom.nonQuery (Query.update tableName) [ idParam docId; jsonParam "@data" document ] conn
        
        /// Update an entire document by its ID, using the provided function to obtain the ID from the document
        [<CompiledName "FSharpByFunc">]
        let byFunc tableName (idFunc: 'TDoc -> 'TKey) (document: 'TDoc) conn =
            byId tableName (idFunc document) document conn
        
        /// Update an entire document by its ID, using the provided function to obtain the ID from the document
        let ByFunc(tableName, idFunc: System.Func<'TDoc, 'TKey>, document: 'TDoc, conn) =
            byFunc tableName idFunc.Invoke document conn
    
    /// Commands to patch (partially update) documents
    [<RequireQualifiedAccess>]
    module Patch =
        
        /// Patch a document by its ID
        [<CompiledName "ById">]
        let byId tableName (docId: 'TKey) (patch: 'TPatch) conn =
            Custom.nonQuery (Query.Patch.byId tableName) [ idParam docId; jsonParam "@data" patch ] conn
        
        /// Patch documents using a comparison on a JSON field
        [<CompiledName "ByField">]
        let byField tableName field (patch: 'TPatch) (conn: SqliteConnection) =
            Custom.nonQuery
                (Query.Patch.byField tableName field) (addFieldParam "@field" field [ jsonParam "@data" patch ]) conn

    /// Commands to remove fields from documents
    [<RequireQualifiedAccess>]
    module RemoveFields =
        
        /// Remove fields from a document by the document's ID
        [<CompiledName "FSharpById">]
        let byId tableName (docId: 'TKey) fieldNames conn =
            let nameParams = fieldNameParams "@name" fieldNames
            Custom.nonQuery (Query.RemoveFields.byId tableName nameParams) (idParam docId :: nameParams) conn
        
        /// Remove fields from a document by the document's ID
        let ById(tableName, docId: 'TKey, fieldNames, conn) =
            byId tableName docId (List.ofSeq fieldNames) conn
        
        /// Remove fields from documents via a comparison on a JSON field in the document
        [<CompiledName "FSharpByField">]
        let byField tableName field fieldNames conn =
            let nameParams = fieldNameParams "@name" fieldNames
            Custom.nonQuery
                (Query.RemoveFields.byField tableName field nameParams) (addFieldParam "@field" field nameParams) conn
        
        /// Remove fields from documents via a comparison on a JSON field in the document
        let ByField(tableName, field, fieldNames, conn) =
            byField tableName field (List.ofSeq fieldNames) conn
    
    /// Commands to delete documents
    [<RequireQualifiedAccess>]
    module Delete =
        
        /// Delete a document by its ID
        [<CompiledName "ById">]
        let byId tableName (docId: 'TKey) conn =
            Custom.nonQuery (Query.Delete.byId tableName) [ idParam docId ] conn

        /// Delete documents by matching a comparison on a JSON field
        [<CompiledName "ByField">]
        let byField tableName field conn =
            Custom.nonQuery (Query.Delete.byField tableName field) (addFieldParam "@field" field []) conn


/// Commands to execute custom SQL queries
[<RequireQualifiedAccess>]
module Custom =

    /// Execute a query that returns a list of results
    [<CompiledName "FSharpList">]
    let list<'TDoc> query parameters (mapFunc: SqliteDataReader -> 'TDoc) =
        use conn = Configuration.dbConn ()
        WithConn.Custom.list<'TDoc> query parameters mapFunc conn

    /// Execute a query that returns a list of results
    let List<'TDoc>(query, parameters, mapFunc: System.Func<SqliteDataReader, 'TDoc>) =
        use conn = Configuration.dbConn ()
        WithConn.Custom.List<'TDoc>(query, parameters, mapFunc, conn)

    /// Execute a query that returns one or no results (returns None if not found)
    [<CompiledName "FSharpSingle">]
    let single<'TDoc> query parameters (mapFunc: SqliteDataReader -> 'TDoc) =
        use conn = Configuration.dbConn ()
        WithConn.Custom.single<'TDoc> query parameters mapFunc conn

    /// Execute a query that returns one or no results (returns null if not found)
    let Single<'TDoc when 'TDoc: null>(query, parameters, mapFunc: System.Func<SqliteDataReader, 'TDoc>) =
        use conn = Configuration.dbConn ()
        WithConn.Custom.Single<'TDoc>(query, parameters, mapFunc, conn)

    /// Execute a query that does not return a value
    [<CompiledName "NonQuery">]
    let nonQuery query parameters =
        use conn = Configuration.dbConn ()
        WithConn.Custom.nonQuery query parameters conn
    
    /// Execute a query that returns a scalar value
    [<CompiledName "FSharpScalar">]
    let scalar<'T when 'T: struct> query parameters (mapFunc: SqliteDataReader -> 'T) =
        use conn = Configuration.dbConn ()
        WithConn.Custom.scalar<'T> query parameters mapFunc conn

    /// Execute a query that returns a scalar value
    let Scalar<'T when 'T: struct>(query, parameters, mapFunc: System.Func<SqliteDataReader, 'T>) =
        use conn = Configuration.dbConn ()
        WithConn.Custom.Scalar<'T>(query, parameters, mapFunc, conn)

/// Functions to create tables and indexes
[<RequireQualifiedAccess>]
module Definition =

    /// Create a document table
    [<CompiledName "EnsureTable">]
    let ensureTable name =
        use conn = Configuration.dbConn ()
        WithConn.Definition.ensureTable name conn
    
    /// Create an index on a document table
    [<CompiledName "EnsureFieldIndex">]
    let ensureFieldIndex tableName indexName fields =
        use conn = Configuration.dbConn ()
        WithConn.Definition.ensureFieldIndex tableName indexName fields conn

/// Document insert/save functions
[<AutoOpen>]
module Document =
    
    /// Insert a new document
    [<CompiledName "Insert">]
    let insert<'TDoc> tableName (document: 'TDoc) =
        use conn = Configuration.dbConn ()
        WithConn.insert tableName document conn

    /// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
    [<CompiledName "Save">]
    let save<'TDoc> tableName (document: 'TDoc) =
        use conn = Configuration.dbConn ()
        WithConn.save tableName document conn

/// Commands to count documents
[<RequireQualifiedAccess>]
module Count =
    
    /// Count all documents in a table
    [<CompiledName "All">]
    let all tableName =
        use conn = Configuration.dbConn ()
        WithConn.Count.all tableName conn
    
    /// Count matching documents using a comparison on a JSON field
    [<CompiledName "ByField">]
    let byField tableName field =
        use conn = Configuration.dbConn ()
        WithConn.Count.byField tableName field conn

/// Commands to determine if documents exist
[<RequireQualifiedAccess>]
module Exists =

    /// Determine if a document exists for the given ID
    [<CompiledName "ById">]
    let byId tableName (docId: 'TKey) =
        use conn = Configuration.dbConn ()
        WithConn.Exists.byId tableName docId conn

    /// Determine if a document exists using a comparison on a JSON field
    [<CompiledName "ByField">]
    let byField tableName field =
        use conn = Configuration.dbConn ()
        WithConn.Exists.byField tableName field conn

/// Commands to determine if documents exist
[<RequireQualifiedAccess>]
module Find =
    
    /// Retrieve all documents in the given table
    [<CompiledName "FSharpAll">]
    let all<'TDoc> tableName =
        use conn = Configuration.dbConn ()
        WithConn.Find.all<'TDoc> tableName conn

    /// Retrieve all documents in the given table
    let All<'TDoc> tableName =
        use conn = Configuration.dbConn ()
        WithConn.Find.All<'TDoc>(tableName, conn)

    /// Retrieve a document by its ID (returns None if not found)
    [<CompiledName "FSharpById">]
    let byId<'TKey, 'TDoc> tableName docId =
        use conn = Configuration.dbConn ()
        WithConn.Find.byId<'TKey, 'TDoc> tableName docId conn

    /// Retrieve a document by its ID (returns null if not found)
    let ById<'TKey, 'TDoc when 'TDoc: null>(tableName, docId) =
        use conn = Configuration.dbConn ()
        WithConn.Find.ById<'TKey, 'TDoc>(tableName, docId, conn)

    /// Retrieve documents via a comparison on a JSON field
    [<CompiledName "FSharpByField">]
    let byField<'TDoc> tableName field =
        use conn = Configuration.dbConn ()
        WithConn.Find.byField<'TDoc> tableName field conn

    /// Retrieve documents via a comparison on a JSON field
    let ByField<'TDoc>(tableName, field) =
        use conn = Configuration.dbConn ()
        WithConn.Find.ByField<'TDoc>(tableName, field, conn)

    /// Retrieve documents via a comparison on a JSON field, returning only the first result
    [<CompiledName "FSharpFirstByField">]
    let firstByField<'TDoc> tableName field =
        use conn = Configuration.dbConn ()
        WithConn.Find.firstByField<'TDoc> tableName field conn

    /// Retrieve documents via a comparison on a JSON field, returning only the first result
    let FirstByField<'TDoc when 'TDoc: null>(tableName, field) =
        use conn = Configuration.dbConn ()
        WithConn.Find.FirstByField<'TDoc>(tableName, field, conn)

/// Commands to update documents
[<RequireQualifiedAccess>]
module Update =
    
    /// Update an entire document by its ID
    [<CompiledName "ById">]
    let byId tableName (docId: 'TKey) (document: 'TDoc) =
        use conn = Configuration.dbConn ()
        WithConn.Update.byId tableName docId document conn
    
    /// Update an entire document by its ID, using the provided function to obtain the ID from the document
    [<CompiledName "FSharpByFunc">]
    let byFunc tableName (idFunc: 'TDoc -> 'TKey) (document: 'TDoc) =
        use conn = Configuration.dbConn ()
        WithConn.Update.byFunc tableName idFunc document conn
    
    /// Update an entire document by its ID, using the provided function to obtain the ID from the document
    let ByFunc(tableName, idFunc: System.Func<'TDoc, 'TKey>, document: 'TDoc) =
        use conn = Configuration.dbConn ()
        WithConn.Update.ByFunc(tableName, idFunc, document, conn)

/// Commands to patch (partially update) documents
[<RequireQualifiedAccess>]
module Patch =
    
    /// Patch a document by its ID
    [<CompiledName "ById">]
    let byId tableName (docId: 'TKey) (patch: 'TPatch) =
        use conn = Configuration.dbConn ()
        WithConn.Patch.byId tableName docId patch conn
    
    /// Patch documents using a comparison on a JSON field in the WHERE clause
    [<CompiledName "ByField">]
    let byField tableName field (patch: 'TPatch) =
        use conn = Configuration.dbConn ()
        WithConn.Patch.byField tableName field patch conn

/// Commands to remove fields from documents
[<RequireQualifiedAccess>]
module RemoveFields =
    
    /// Remove fields from a document by the document's ID
    [<CompiledName "FSharpById">]
    let byId tableName (docId: 'TKey) fieldNames =
        use conn = Configuration.dbConn ()
        WithConn.RemoveFields.byId tableName docId fieldNames conn
        
    /// Remove fields from a document by the document's ID
    let ById(tableName, docId: 'TKey, fieldNames) =
        use conn = Configuration.dbConn ()
        WithConn.RemoveFields.ById(tableName, docId, fieldNames, conn)
        
    /// Remove field from documents via a comparison on a JSON field in the document
    [<CompiledName "FSharpByField">]
    let byField tableName field fieldNames =
        use conn = Configuration.dbConn ()
        WithConn.RemoveFields.byField tableName field fieldNames conn

    /// Remove field from documents via a comparison on a JSON field in the document
    let ByField(tableName, field, fieldNames) =
        use conn = Configuration.dbConn ()
        WithConn.RemoveFields.ByField(tableName, field, fieldNames, conn)

/// Commands to delete documents
[<RequireQualifiedAccess>]
module Delete =
    
    /// Delete a document by its ID
    [<CompiledName "ById">]
    let byId tableName (docId: 'TKey) =
        use conn = Configuration.dbConn ()
        WithConn.Delete.byId tableName docId conn

    /// Delete documents by matching a comparison on a JSON field
    [<CompiledName "ByField">]
    let byField tableName field =
        use conn = Configuration.dbConn ()
        WithConn.Delete.byField tableName field conn
