package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonObject
import org.bson.BsonDocument
import org.bson.BsonString

data class QueryCommand(val db: String, val collection: String, val filter: JsonObject, val sort: JsonObject) {
    val namespace: String
        get() = "$db.$collection"
}

class Query(doc: JsonObject) : ReadOperation(doc) {
    fun command() : QueryCommand {
        val command = when {
            doc.containsKey("command") -> doc.obj("command")!!
            doc.containsKey("query") -> {
                if (doc["query"] is JsonObject) {
                    doc.obj("query")!!
                } else {
                    throw java.lang.Exception("Query command filter is redacted")
                }
            }
            else -> throw Exception("unexpected query profile document format, could not find either the [\"command\", \"query\"] field")
        }

        var sort = JsonObject()

        if (command.containsKey("sort")) {
            sort = command.obj("sort")!!
        }

        val filter: JsonObject

        if (command.containsKey("filter")) {
            filter = command.obj("filter")!!
        } else {
            filter = JsonObject()
        }

        return QueryCommand(
            namespace().db,
            namespace().collection,
            filter,
            sort)
    }

    fun numberDeleted() = doc.int("nDeleted")
}