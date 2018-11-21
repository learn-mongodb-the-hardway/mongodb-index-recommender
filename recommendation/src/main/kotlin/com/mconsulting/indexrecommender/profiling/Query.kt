package com.mconsulting.indexrecommender.profiling

import org.bson.BsonDocument
import org.bson.BsonString

data class QueryCommand(val db: String, val collection: String, val filter: BsonDocument, val sort: BsonDocument)

class Query(doc: BsonDocument) : ReadOperation(doc) {

    fun command() : QueryCommand {
        var command: BsonDocument

        if (doc.containsKey("command")) {
            command = doc.getDocument("command")
        } else if (doc.containsKey("query")) {
            command = doc.getDocument("query")
        } else {
            throw Exception("unexpected query profile document format, could not find either the [\"command\", \"query\"] field")
        }

        var sort = BsonDocument()

        if (command.containsKey("sort")) {
            sort = command.getDocument("sort")
        }

        return QueryCommand(
            namespace().db,
            command.getString("find").value,
            command.getDocument("filter"),
            sort)
    }

    fun numberDeleted() = doc.getInt32("nDeleted")
}