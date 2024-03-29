﻿namespace BitBadger.Documents

/// The types of logical operations available for JSON fields
[<Struct>]
type Op =
    /// Equals (=)
    | EQ
    /// Greater Than (>)
    | GT
    /// Greater Than or Equal To (>=)
    | GE
    /// Less Than (<)
    | LT
    /// Less Than or Equal To (<=)
    | LE
    /// Not Equal to (<>)
    | NE
    /// Exists (IS NOT NULL)
    | EX
    /// Does Not Exist (IS NULL)
    | NEX
    
    override this.ToString() =
        match this with
        | EQ -> "="
        | GT -> ">"
        | GE -> ">="
        | LT -> "<"
        | LE -> "<="
        | NE -> "<>"
        | EX -> "IS NOT NULL"
        | NEX -> "IS NULL"


/// Criteria for a field WHERE clause
type Field = {
    /// The name of the field
    Name: string
    
    /// The operation by which the field will be compared
    Op: Op
    
    /// The value of the field
    Value: obj
} with
    
    /// Create an equals (=) field criterion
    static member EQ name (value: obj) =
        { Name = name; Op = EQ; Value = value }
    
    /// Create a greater than (>) field criterion
    static member GT name (value: obj) =
        { Name = name; Op = GT; Value = value }
    
    /// Create a greater than or equal to (>=) field criterion
    static member GE name (value: obj) =
        { Name = name; Op = GE; Value = value }
    
    /// Create a less than (<) field criterion
    static member LT name (value: obj) =
        { Name = name; Op = LT; Value = value }
    
    /// Create a less than or equal to (<=) field criterion
    static member LE name (value: obj) =
        { Name = name; Op = LE; Value = value }
    
    /// Create a not equals (<>) field criterion
    static member NE name (value: obj) =
        { Name = name; Op = NE; Value = value }
    
    /// Create an exists (IS NOT NULL) field criterion
    static member EX name =
        { Name = name; Op = EX; Value = obj () }
    
    /// Create an not exists (IS NULL) field criterion
    static member NEX name =
        { Name = name; Op = NEX; Value = obj () }


/// The required document serialization implementation
type IDocumentSerializer =
    
    /// Serialize an object to a JSON string
    abstract Serialize<'T> : 'T -> string
    
    /// Deserialize a JSON string into an object
    abstract Deserialize<'T> : string -> 'T


/// Document serializer defaults 
module DocumentSerializer =
    
    open System.Text.Json
    open System.Text.Json.Serialization
    
    /// The default JSON serializer options to use with the stock serializer
    let private jsonDefaultOpts =
        let o = JsonSerializerOptions()
        o.Converters.Add(JsonFSharpConverter())
        o
    
    /// The default JSON serializer
    [<CompiledName "Default">]
    let ``default`` =
        { new IDocumentSerializer with
            member _.Serialize<'T>(it: 'T) : string =
                JsonSerializer.Serialize(it, jsonDefaultOpts)
            member _.Deserialize<'T>(it: string) : 'T =
                JsonSerializer.Deserialize<'T>(it, jsonDefaultOpts)
        }


/// Configuration for document handling
[<RequireQualifiedAccess>]
module Configuration =

    /// The serializer to use for document manipulation
    let mutable private serializerValue = DocumentSerializer.``default``
    
    /// Register a serializer to use for translating documents to domain types
    [<CompiledName "UseSerializer">]
    let useSerializer ser =
        serializerValue <- ser

    /// Retrieve the currently configured serializer
    [<CompiledName "Serializer">]
    let serializer () =
        serializerValue
    
    /// The serialized name of the ID field for documents
    let mutable idFieldValue = "Id"
    
    /// Specify the name of the ID field for documents
    [<CompiledName "UseIdField">]
    let useIdField it =
        idFieldValue <- it
    
    /// Retrieve the currently configured ID field for documents
    [<CompiledName "IdField">]
    let idField () =
        idFieldValue


/// Query construction functions
[<RequireQualifiedAccess>]
module Query =
    
    /// Create a SELECT clause to retrieve the document data from the given table
    [<CompiledName "SelectFromTable">]
    let selectFromTable tableName =
        $"SELECT data FROM %s{tableName}"
    
    /// Create a WHERE clause fragment to implement a comparison on a field in a JSON document
    [<CompiledName "WhereByField">]
    let whereByField field paramName =
        let theRest = match field.Op with EX | NEX -> string field.Op | _ -> $"{field.Op} %s{paramName}"
        $"data ->> '%s{field.Name}' {theRest}"
    
    /// Create a WHERE clause fragment to implement an ID-based query
    [<CompiledName "WhereById">]
    let whereById paramName =
        whereByField (Field.EQ (Configuration.idField ()) 0) paramName
    
    /// Queries to define tables and indexes
    module Definition =
        
        /// SQL statement to create a document table
        [<CompiledName "EnsureTableFor">]
        let ensureTableFor name dataType =
            $"CREATE TABLE IF NOT EXISTS %s{name} (data %s{dataType} NOT NULL)"
        
        /// Split a schema and table name
        let private splitSchemaAndTable (tableName: string) =
            let parts = tableName.Split '.'
            if Array.length parts = 1 then "", tableName else parts[0], parts[1]
        
        /// SQL statement to create an index on one or more fields in a JSON document
        [<CompiledName "EnsureIndexOn">]
        let ensureIndexOn tableName indexName (fields: string seq) =
            let _, tbl = splitSchemaAndTable tableName
            let jsonFields =
                fields
                |> Seq.map (fun it ->
                    let parts = it.Split ' '
                    let fieldName = if Array.length parts = 1 then it else parts[0]
                    let direction = if Array.length parts < 2 then "" else $" {parts[1]}"
                    $"(data ->> '{fieldName}'){direction}")
                |> String.concat ", "
            $"CREATE INDEX IF NOT EXISTS idx_{tbl}_%s{indexName} ON {tableName} ({jsonFields})"

        /// SQL statement to create a key index for a document table
        [<CompiledName "EnsureKey">]
        let ensureKey tableName =
            (ensureIndexOn tableName "key" [ Configuration.idField () ]).Replace("INDEX", "UNIQUE INDEX")
        
    /// Query to insert a document
    [<CompiledName "Insert">]
    let insert tableName =
        $"INSERT INTO %s{tableName} VALUES (@data)"

    /// Query to save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
    [<CompiledName "Save">]
    let save tableName =
        sprintf
            "INSERT INTO %s VALUES (@data) ON CONFLICT ((data ->> '%s')) DO UPDATE SET data = EXCLUDED.data"
            tableName (Configuration.idField ()) 
    
    /// Query to update a document
    [<CompiledName "Update">]
    let update tableName =
        $"""UPDATE %s{tableName} SET data = @data WHERE {whereById "@id"}"""

    /// Queries for counting documents
    module Count =
        
        /// Query to count all documents in a table
        [<CompiledName "All">]
        let all tableName =
            $"SELECT COUNT(*) AS it FROM %s{tableName}"
        
        /// Query to count matching documents using a text comparison on a JSON field
        [<CompiledName "ByField">]
        let byField tableName field =
            $"""SELECT COUNT(*) AS it FROM %s{tableName} WHERE {whereByField field "@field"}"""
        
    /// Queries for determining document existence
    module Exists =

        /// Query to determine if a document exists for the given ID
        [<CompiledName "ById">]
        let byId tableName =
            $"""SELECT EXISTS (SELECT 1 FROM %s{tableName} WHERE {whereById "@id"}) AS it"""

        /// Query to determine if documents exist using a comparison on a JSON field
        [<CompiledName "ByField">]
        let byField tableName field =
            $"""SELECT EXISTS (SELECT 1 FROM %s{tableName} WHERE {whereByField field "@field"}) AS it"""
        
    /// Queries for retrieving documents
    module Find =

        /// Query to retrieve a document by its ID
        [<CompiledName "ById">]
        let byId tableName =
            $"""{selectFromTable tableName} WHERE {whereById "@id"}"""
        
        /// Query to retrieve documents using a comparison on a JSON field
        [<CompiledName "ByField">]
        let byField tableName field =
            $"""{selectFromTable tableName} WHERE {whereByField field "@field"}"""
        
    /// Queries to delete documents
    module Delete =
        
        /// Query to delete a document by its ID
        [<CompiledName "ById">]
        let byId tableName =
            $"""DELETE FROM %s{tableName} WHERE {whereById "@id"}"""

        /// Query to delete documents using a comparison on a JSON field
        [<CompiledName "ByField">]
        let byField tableName field =
            $"""DELETE FROM %s{tableName} WHERE {whereByField field "@field"}"""
