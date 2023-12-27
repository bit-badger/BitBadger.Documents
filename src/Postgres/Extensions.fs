namespace BitBadger.Documents.Postgres

open Npgsql
open Npgsql.FSharp

/// F# Extensions for the NpgsqlConnection type 
[<AutoOpen>]
module Extensions =
    
    type NpgsqlConnection with

        /// Execute a query that returns a list of results
        member conn.customList<'TDoc> query parameters (mapFunc: RowReader -> 'TDoc) =
            WithProps.Custom.list<'TDoc> query parameters mapFunc (Sql.existingConnection conn)

        /// Execute a query that returns one or no results; returns None if not found
        member conn.customSingle<'TDoc> query parameters (mapFunc: RowReader ->  'TDoc) =
            WithProps.Custom.single<'TDoc> query parameters mapFunc (Sql.existingConnection conn)

        /// Execute a query that returns no results
        member conn.customNonQuery query parameters =
            WithProps.Custom.nonQuery query parameters (Sql.existingConnection conn)

        /// Execute a query that returns a scalar value
        member conn.customScalar<'T when 'T: struct> query parameters (mapFunc: RowReader -> 'T) =
            WithProps.Custom.scalar query parameters mapFunc (Sql.existingConnection conn)
        
        /// Create a document table
        member conn.ensureTable name =
            WithProps.Definition.ensureTable name (Sql.existingConnection conn)

        /// Create an index on documents in the specified table
        member conn.ensureDocumentIndex name idxType =
            WithProps.Definition.ensureDocumentIndex name idxType (Sql.existingConnection conn)
        
        /// Create an index on field(s) within documents in the specified table
        member conn.ensureFieldIndex tableName indexName fields =
            WithProps.Definition.ensureFieldIndex tableName indexName fields (Sql.existingConnection conn)

        /// Insert a new document
        member conn.insert<'TDoc> tableName (document: 'TDoc) =
            WithProps.Document.insert<'TDoc> tableName document (Sql.existingConnection conn)

        /// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
        member conn.save<'TDoc> tableName (document: 'TDoc) =
            WithProps.Document.save<'TDoc> tableName document (Sql.existingConnection conn)

        /// Count all documents in a table
        member conn.countAll tableName =
            WithProps.Count.all tableName (Sql.existingConnection conn)
        
        /// Count matching documents using a JSON field comparison query (->> =)
        member conn.countByField tableName fieldName op (value: obj) =
            WithProps.Count.byField tableName fieldName op value (Sql.existingConnection conn)
        
        /// Count matching documents using a JSON containment query (@>)
        member conn.countByContains tableName criteria =
            WithProps.Count.byContains tableName criteria (Sql.existingConnection conn)

        /// Count matching documents using a JSON Path match query (@?)
        member conn.countByJsonPath tableName jsonPath =
            WithProps.Count.byJsonPath tableName jsonPath (Sql.existingConnection conn)

        /// Determine if a document exists for the given ID
        member conn.existsById tableName docId =
            WithProps.Exists.byId tableName docId (Sql.existingConnection conn)
        
        /// Determine if documents exist using a JSON field comparison query (->> =)
        member conn.existsByField tableName fieldName op (value: obj) =
            WithProps.Exists.byField tableName fieldName op value (Sql.existingConnection conn)
        
        /// Determine if documents exist using a JSON containment query (@>)
        member conn.existsByContains tableName criteria =
            WithProps.Exists.byContains tableName criteria (Sql.existingConnection conn)

        /// Determine if documents exist using a JSON Path match query (@?)
        member conn.existsByJsonPath tableName jsonPath =
            WithProps.Exists.byJsonPath tableName jsonPath (Sql.existingConnection conn)
        
        /// Retrieve all documents in the given table
        member conn.findAll<'TDoc> tableName =
            WithProps.Find.all<'TDoc> tableName (Sql.existingConnection conn)

        /// Retrieve a document by its ID; returns None if not found
        member conn.findById<'TKey, 'TDoc> tableName docId =
            WithProps.Find.byId<'TKey, 'TDoc> tableName docId (Sql.existingConnection conn)

        /// Retrieve documents matching a JSON field comparison query (->> =)
        member conn.findByField<'TDoc> tableName fieldName op (value: obj) =
            WithProps.Find.byField<'TDoc> tableName fieldName op value (Sql.existingConnection conn)
        
        /// Retrieve documents matching a JSON containment query (@>)
        member conn.findByContains<'TDoc> tableName (criteria: obj) =
            WithProps.Find.byContains<'TDoc> tableName criteria (Sql.existingConnection conn)

        /// Retrieve documents matching a JSON Path match query (@?)
        member conn.findByJsonPath<'TDoc> tableName jsonPath =
            WithProps.Find.byJsonPath<'TDoc> tableName jsonPath (Sql.existingConnection conn)
        
        /// Retrieve the first document matching a JSON field comparison query (->> =); returns None if not found
        member conn.findFirstByField<'TDoc> tableName fieldName op (value: obj) =
            WithProps.Find.firstByField<'TDoc> tableName fieldName op value (Sql.existingConnection conn)
        
        /// Retrieve the first document matching a JSON containment query (@>); returns None if not found
        member conn.findFirstByContains<'TDoc> tableName (criteria: obj) =
            WithProps.Find.firstByContains<'TDoc> tableName criteria (Sql.existingConnection conn)

        /// Retrieve the first document matching a JSON Path match query (@?); returns None if not found
        member conn.findFirstByJsonPath<'TDoc> tableName jsonPath =
            WithProps.Find.firstByJsonPath<'TDoc> tableName jsonPath (Sql.existingConnection conn)
        
        /// Update a full document
        member conn.updateFull tableName (docId: 'TKey) (document: 'TDoc) =
            WithProps.Update.full tableName docId document (Sql.existingConnection conn)

        /// Update a full document
        member conn.updateFullFunc tableName (idFunc: 'TDoc -> 'TKey) (document: 'TDoc) =
            WithProps.Update.fullFunc tableName idFunc document (Sql.existingConnection conn)

        /// Update a partial document
        member conn.updatePartialById tableName (docId: 'TKey) (partial: 'TPartial) =
            WithProps.Update.partialById tableName docId partial (Sql.existingConnection conn)
        
        /// Update partial documents using a JSON field comparison query in the WHERE clause (->> =)
        member conn.updatePartialByField tableName fieldName op (value: obj) (partial: 'TPartial) =
            WithProps.Update.partialByField tableName fieldName op value partial (Sql.existingConnection conn)
        
        /// Update partial documents using a JSON containment query in the WHERE clause (@>)
        member conn.updatePartialByContains tableName (criteria: 'TCriteria) (partial: 'TPartial) =
            WithProps.Update.partialByContains tableName criteria partial (Sql.existingConnection conn)
        
        /// Update partial documents using a JSON Path match query in the WHERE clause (@?)
        member conn.updatePartialByJsonPath tableName jsonPath (partial: 'TPartial) =
            WithProps.Update.partialByJsonPath tableName jsonPath partial (Sql.existingConnection conn)
        
        /// Delete a document by its ID
        member conn.deleteById tableName (docId: 'TKey) =
            WithProps.Delete.byId tableName docId (Sql.existingConnection conn)

        /// Delete documents by matching a JSON field comparison query (->> =)
        member conn.deleteByField tableName fieldName op (value: obj) =
            WithProps.Delete.byField tableName fieldName op value (Sql.existingConnection conn)
        
        /// Delete documents by matching a JSON containment query (@>)
        member conn.deleteByContains tableName (criteria: 'TContains) =
            WithProps.Delete.byContains tableName criteria (Sql.existingConnection conn)

        /// Delete documents by matching a JSON Path match query (@?)
        member conn.deleteByJsonPath tableName path =
            WithProps.Delete.byJsonPath tableName path (Sql.existingConnection conn)


open System.Runtime.CompilerServices

/// C# extensions on the NpgsqlConnection type
type NpgsqlConnectionCSharpExtensions =
    
    /// Execute a query that returns a list of results
    [<Extension>]
    static member inline CustomList<'TDoc>(conn, query, parameters, mapFunc: System.Func<RowReader, 'TDoc>) =
        WithProps.Custom.List<'TDoc>(query, parameters, mapFunc, Sql.existingConnection conn)

    /// Execute a query that returns one or no results; returns None if not found
    [<Extension>]
    static member inline CustomSingle<'TDoc when 'TDoc: null>(
            conn, query, parameters, mapFunc: System.Func<RowReader, 'TDoc>) =
        WithProps.Custom.Single<'TDoc>(query, parameters, mapFunc, Sql.existingConnection conn)

    /// Execute a query that returns no results
    [<Extension>]
    static member inline CustomNonQuery(conn, query, parameters) =
        WithProps.Custom.nonQuery query parameters (Sql.existingConnection conn)

    /// Execute a query that returns a scalar value
    [<Extension>]
    static member inline CustomScalar<'T when 'T: struct>(
            conn, query, parameters, mapFunc: System.Func<RowReader, 'T>) =
        WithProps.Custom.Scalar(query, parameters, mapFunc, Sql.existingConnection conn)
    
    /// Create a document table
    [<Extension>]
    static member inline EnsureTable(conn, name) =
        WithProps.Definition.ensureTable name (Sql.existingConnection conn)

    /// Create an index on documents in the specified table
    [<Extension>]
    static member inline EnsureDocumentIndex(conn, name, idxType) =
        WithProps.Definition.ensureDocumentIndex name idxType (Sql.existingConnection conn)
    
    /// Create an index on field(s) within documents in the specified table
    [<Extension>]
    static member inline EnsureFieldIndex(conn, tableName, indexName, fields) =
        WithProps.Definition.ensureFieldIndex tableName indexName fields (Sql.existingConnection conn)

    /// Insert a new document
    [<Extension>]
    static member inline Insert<'TDoc>(conn, tableName, document: 'TDoc) =
        WithProps.Document.insert<'TDoc> tableName document (Sql.existingConnection conn)

    /// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
    [<Extension>]
    static member inline Save<'TDoc>(conn, tableName, document: 'TDoc) =
        WithProps.Document.save<'TDoc> tableName document (Sql.existingConnection conn)

    /// Count all documents in a table
    [<Extension>]
    static member inline CountAll(conn, tableName) =
        WithProps.Count.all tableName (Sql.existingConnection conn)
    
    /// Count matching documents using a JSON field comparison query (->> =)
    [<Extension>]
    static member inline CountByField(conn, tableName, fieldName, op, value: obj) =
        WithProps.Count.byField tableName fieldName op value (Sql.existingConnection conn)
    
    /// Count matching documents using a JSON containment query (@>)
    [<Extension>]
    static member inline CountByContains(conn, tableName, criteria: 'TCriteria) =
        WithProps.Count.byContains tableName criteria (Sql.existingConnection conn)

    /// Count matching documents using a JSON Path match query (@?)
    [<Extension>]
    static member inline CountByJsonPath(conn, tableName, jsonPath) =
        WithProps.Count.byJsonPath tableName jsonPath (Sql.existingConnection conn)

    /// Determine if a document exists for the given ID
    [<Extension>]
    static member inline ExistsById(conn, tableName, docId) =
        WithProps.Exists.byId tableName docId (Sql.existingConnection conn)
    
    /// Determine if documents exist using a JSON field comparison query (->> =)
    [<Extension>]
    static member inline ExistsByField(conn, tableName, fieldName, op, value: obj) =
        WithProps.Exists.byField tableName fieldName op value (Sql.existingConnection conn)
    
    /// Determine if documents exist using a JSON containment query (@>)
    [<Extension>]
    static member inline ExistsByContains(conn, tableName, criteria: 'TCriteria) =
        WithProps.Exists.byContains tableName criteria (Sql.existingConnection conn)

    /// Determine if documents exist using a JSON Path match query (@?)
    [<Extension>]
    static member inline ExistsByJsonPath(conn, tableName, jsonPath) =
        WithProps.Exists.byJsonPath tableName jsonPath (Sql.existingConnection conn)
    
    /// Retrieve all documents in the given table
    [<Extension>]
    static member inline FindAll<'TDoc>(conn, tableName) =
        WithProps.Find.All<'TDoc>(tableName, Sql.existingConnection conn)

    /// Retrieve a document by its ID; returns None if not found
    [<Extension>]
    static member inline FindById<'TKey, 'TDoc when 'TDoc: null>(conn, tableName, docId: 'TKey) =
        WithProps.Find.ById<'TKey, 'TDoc>(tableName, docId, Sql.existingConnection conn)

    /// Retrieve documents matching a JSON field comparison query (->> =)
    [<Extension>]
    static member inline FindByField<'TDoc>(conn, tableName, fieldName, op, value: obj) =
        WithProps.Find.ByField<'TDoc>(tableName, fieldName, op, value, Sql.existingConnection conn)
    
    /// Retrieve documents matching a JSON containment query (@>)
    [<Extension>]
    static member inline FindByContains<'TDoc>(conn, tableName, criteria: obj) =
        WithProps.Find.ByContains<'TDoc>(tableName, criteria, Sql.existingConnection conn)

    /// Retrieve documents matching a JSON Path match query (@?)
    [<Extension>]
    static member inline FindByJsonPath<'TDoc>(conn, tableName, jsonPath) =
        WithProps.Find.ByJsonPath<'TDoc>(tableName, jsonPath, Sql.existingConnection conn)
    
    /// Retrieve the first document matching a JSON field comparison query (->> =); returns None if not found
    [<Extension>]
    static member inline FindFirstByField<'TDoc when 'TDoc: null>(conn, tableName, fieldName, op, value: obj) =
        WithProps.Find.FirstByField<'TDoc>(tableName, fieldName, op, value, Sql.existingConnection conn)
    
    /// Retrieve the first document matching a JSON containment query (@>); returns None if not found
    [<Extension>]
    static member inline FindFirstByContains<'TDoc when 'TDoc: null>(conn, tableName, criteria: obj) =
        WithProps.Find.FirstByContains<'TDoc>(tableName, criteria, Sql.existingConnection conn)

    /// Retrieve the first document matching a JSON Path match query (@?); returns None if not found
    [<Extension>]
    static member inline FindFirstByJsonPath<'TDoc when 'TDoc: null>(conn, tableName, jsonPath) =
        WithProps.Find.FirstByJsonPath<'TDoc>(tableName, jsonPath, Sql.existingConnection conn)
    
    /// Update a full document
    [<Extension>]
    static member inline UpdateFull(conn, tableName, docId: 'TKey, document: 'TDoc) =
        WithProps.Update.full tableName docId document (Sql.existingConnection conn)

    /// Update a full document
    [<Extension>]
    static member inline UpdateFullFunc(conn, tableName, idFunc: System.Func<'TDoc, 'TKey>, document: 'TDoc) =
        WithProps.Update.FullFunc(tableName, idFunc, document, Sql.existingConnection conn)

    /// Update a partial document
    [<Extension>]
    static member inline UpdatePartialById(conn, tableName, docId: 'TKey, partial: 'TPartial) =
        WithProps.Update.partialById tableName docId partial (Sql.existingConnection conn)
    
    /// Update partial documents using a JSON field comparison query in the WHERE clause (->> =)
    [<Extension>]
    static member inline UpdatePartialByField(conn, tableName, fieldName, op, value: obj, partial: 'TPartial) =
        WithProps.Update.partialByField tableName fieldName op value partial (Sql.existingConnection conn)
    
    /// Update partial documents using a JSON containment query in the WHERE clause (@>)
    [<Extension>]
    static member inline UpdatePartialByContains(conn, tableName, criteria: 'TCriteria, partial: 'TPartial) =
        WithProps.Update.partialByContains tableName criteria partial (Sql.existingConnection conn)
    
    /// Update partial documents using a JSON Path match query in the WHERE clause (@?)
    [<Extension>]
    static member inline UpdatePartialByJsonPath(conn, tableName, jsonPath, partial: 'TPartial) =
        WithProps.Update.partialByJsonPath tableName jsonPath partial (Sql.existingConnection conn)
    
    /// Delete a document by its ID
    [<Extension>]
    static member inline DeleteById(conn, tableName, docId: 'TKey) =
        WithProps.Delete.byId tableName docId (Sql.existingConnection conn)

    /// Delete documents by matching a JSON field comparison query (->> =)
    [<Extension>]
    static member inline DeleteByField(conn, tableName, fieldName, op, value: obj) =
        WithProps.Delete.byField tableName fieldName op value (Sql.existingConnection conn)
    
    /// Delete documents by matching a JSON containment query (@>)
    [<Extension>]
    static member inline DeleteByContains(conn, tableName, criteria: 'TContains) =
        WithProps.Delete.byContains tableName criteria (Sql.existingConnection conn)

    /// Delete documents by matching a JSON Path match query (@?)
    [<Extension>]
    static member inline DeleteByJsonPath(conn, tableName, path) =
        WithProps.Delete.byJsonPath tableName path (Sql.existingConnection conn)
