namespace BitBadger.Documents

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
    let whereByField fieldName op paramName =
        let theRest =
            match op with
            | EX | NEX -> string op
            | _ -> $"{op} %s{paramName}"
        $"data ->> '%s{fieldName}' {theRest}"
    
    /// Create a WHERE clause fragment to implement an ID-based query
    [<CompiledName "WhereById">]
    let whereById paramName =
        whereByField (Configuration.idField ()) EQ paramName
    
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
    
    /// Queries for counting documents
    module Count =
        
        /// Query to count all documents in a table
        [<CompiledName "All">]
        let all tableName =
            $"SELECT COUNT(*) AS it FROM %s{tableName}"
        
        /// Query to count matching documents using a text comparison on a JSON field
        [<CompiledName "ByField">]
        let byField tableName fieldName op =
            $"""SELECT COUNT(*) AS it FROM %s{tableName} WHERE {whereByField fieldName op "@field"}"""
        
    /// Queries for determining document existence
    module Exists =

        /// Query to determine if a document exists for the given ID
        [<CompiledName "ById">]
        let byId tableName =
            $"""SELECT EXISTS (SELECT 1 FROM %s{tableName} WHERE {whereById "@id"}) AS it"""

        /// Query to determine if documents exist using a comparison on a JSON field
        [<CompiledName "ByField">]
        let byField tableName fieldName op =
            $"""SELECT EXISTS (SELECT 1 FROM %s{tableName} WHERE {whereByField fieldName op "@field"}) AS it"""
        
    /// Queries for retrieving documents
    module Find =

        /// Query to retrieve a document by its ID
        [<CompiledName "ById">]
        let byId tableName =
            $"""{selectFromTable tableName} WHERE {whereById "@id"}"""
        
        /// Query to retrieve documents using a comparison on a JSON field
        [<CompiledName "ByField">]
        let byField tableName fieldName op =
            $"""{selectFromTable tableName} WHERE {whereByField fieldName op "@field"}"""
        
    /// Queries to update documents
    module Update =

        /// Query to update a document
        [<CompiledName "Full">]
        let full tableName =
            $"""UPDATE %s{tableName} SET data = @data WHERE {whereById "@id"}"""

        /// Query to update a partial document by its ID
        [<CompiledName "PartialById">]
        let partialById tableName =
            $"""UPDATE %s{tableName} SET data = json_patch(data, json(@data)) WHERE {whereById "@id"}"""
            
        /// Query to update a partial document via a comparison on a JSON field
        [<CompiledName "PartialByField">]
        let partialByField tableName fieldName op =
            sprintf
                "UPDATE %s SET data = json_patch(data, json(@data)) WHERE %s"
                tableName (whereByField fieldName op "@field")
        
    /// Queries to delete documents
    module Delete =
        
        /// Query to delete a document by its ID
        [<CompiledName "ById">]
        let byId tableName =
            $"""DELETE FROM %s{tableName} WHERE {whereById "@id"}"""

        /// Query to delete documents using a comparison on a JSON field
        [<CompiledName "ByField">]
        let byField tableName fieldName op =
            $"""DELETE FROM %s{tableName} WHERE {whereByField fieldName op "@field"}"""
