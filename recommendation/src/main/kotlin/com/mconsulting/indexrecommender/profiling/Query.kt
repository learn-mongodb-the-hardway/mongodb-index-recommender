package com.mconsulting.indexrecommender.profiling

import org.bson.BsonDocument
import org.bson.BsonString

data class QueryCommand(val db: String, val collection: String, val filter: BsonDocument, val sort: BsonDocument)

class Query(doc: BsonDocument) : ReadOperation(doc) {

    fun command() : QueryCommand {
        val command = doc.getDocument("command")
        var sort: BsonDocument = BsonDocument()

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