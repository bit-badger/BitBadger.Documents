namespace BitBadger.Documents.Sqlite

open Microsoft.Data.Sqlite

/// F# extensions for the SqliteConnection type
[<AutoOpen>]
module Extensions =

    type SqliteConnection with
        
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


open System.Runtime.CompilerServices

/// C# extensions on the SqliteConnection type
type SqliteConnectionCSharpExtensions =
    
    /// Execute a query that returns a list of results
    [<Extension>]
    static member inline CustomList<'TDoc>(conn, query, parameters, mapFunc: System.Func<SqliteDataReader, 'TDoc>) =
        WithConn.Custom.List<'TDoc>(query, parameters, mapFunc, conn)

    /// Execute a query that returns one or no results
    [<Extension>]
    static member inline CustomSingle<'TDoc when 'TDoc: null>(
            conn, query, parameters, mapFunc: System.Func<SqliteDataReader, 'TDoc>) =
        WithConn.Custom.Single<'TDoc>(query, parameters, mapFunc, conn)
    
    /// Execute a query that does not return a value
    [<Extension>]
    static member inline CustomNonQuery(conn, query, parameters) =
        WithConn.Custom.nonQuery query parameters conn

    /// Execute a query that returns a scalar value
    [<Extension>]
    static member inline CustomScalar<'T when 'T: struct>(
            conn, query, parameters, mapFunc: System.Func<SqliteDataReader, 'T>) =
        WithConn.Custom.Scalar<'T>(query, parameters, mapFunc, conn)

    /// Create a document table
    [<Extension>]
    static member inline EnsureTable(conn, name) =
        WithConn.Definition.ensureTable name conn

    /// Create an index on one or more fields in a document table
    [<Extension>]
    static member inline EnsureIndex(conn, tableName, indexName, fields) =
        WithConn.Definition.ensureIndex tableName indexName fields conn

    /// Insert a new document
    [<Extension>]
    static member inline Insert<'TDoc>(conn, tableName, document: 'TDoc) =
        WithConn.insert<'TDoc> tableName document conn

    /// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
    [<Extension>]
    static member inline Save<'TDoc>(conn, tableName, document: 'TDoc) =
        WithConn.save<'TDoc> tableName document conn

    /// Count all documents in a table
    [<Extension>]
    static member inline CountAll(conn, tableName) =
        WithConn.Count.all tableName conn
    
    /// Count matching documents using a comparison on a JSON field
    [<Extension>]
    static member inline CountByField(conn, tableName, fieldName, op, value: obj) =
        WithConn.Count.byField tableName fieldName op value conn

    /// Determine if a document exists for the given ID
    [<Extension>]
    static member inline ExistsById<'TKey>(conn, tableName, docId: 'TKey) =
        WithConn.Exists.byId tableName docId conn

    /// Determine if a document exists using a comparison on a JSON field
    [<Extension>]
    static member inline ExistsByField(conn, tableName, fieldName, op, value: obj) =
        WithConn.Exists.byField tableName fieldName op value conn
    
    /// Retrieve all documents in the given table
    [<Extension>]
    static member inline FindAll<'TDoc>(conn, tableName) =
        WithConn.Find.All<'TDoc>(tableName, conn)

    /// Retrieve a document by its ID
    [<Extension>]
    static member inline FindById<'TKey, 'TDoc when 'TDoc: null>(conn, tableName, docId: 'TKey) =
        WithConn.Find.ById<'TKey, 'TDoc>(tableName, docId, conn)

    /// Retrieve documents via a comparison on a JSON field
    [<Extension>]
    static member inline FindByField<'TDoc>(conn, tableName, fieldName, op, value) =
        WithConn.Find.ByField<'TDoc>(tableName, fieldName, op, value, conn)

    /// Retrieve documents via a comparison on a JSON field, returning only the first result
    [<Extension>]
    static member inline FindFirstByField<'TDoc when 'TDoc: null>(conn, tableName, fieldName, op, value: obj) =
        WithConn.Find.FirstByField<'TDoc>(tableName, fieldName, op, value, conn)

    /// Update an entire document
    [<Extension>]
    static member inline UpdateFull<'TKey, 'TDoc>(conn, tableName, docId: 'TKey, document: 'TDoc) =
        WithConn.Update.full tableName docId document conn
    
    /// Update an entire document
    [<Extension>]
    static member inline UpdateFullFunc<'TKey, 'TDoc>(conn, tableName, idFunc: System.Func<'TDoc, 'TKey>, doc: 'TDoc) =
        WithConn.Update.FullFunc(tableName, idFunc, doc, conn)
    
    /// Update a partial document
    [<Extension>]
    static member inline UpdatePartialById<'TKey, 'TPatch>(conn, tableName, docId: 'TKey, partial: 'TPatch) =
        WithConn.Update.partialById tableName docId partial conn
    
    /// Update partial documents using a comparison on a JSON field
    [<Extension>]
    static member inline UpdatePartialByField<'TPatch>(conn, tableName, fieldName, op, value: obj, partial: 'TPatch) =
        WithConn.Update.partialByField tableName fieldName op value partial conn

    /// Delete a document by its ID
    [<Extension>]
    static member inline DeleteById<'TKey>(conn, tableName, docId: 'TKey) =
        WithConn.Delete.byId tableName docId conn

    /// Delete documents by matching a comparison on a JSON field
    [<Extension>]
    static member inline DeleteByField(conn, tableName, fieldName, op, value: obj) =
        WithConn.Delete.byField tableName fieldName op value conn
