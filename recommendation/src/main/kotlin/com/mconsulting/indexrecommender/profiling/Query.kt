package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonObject
import org.bson.BsonDocument
import org.bson.BsonString

data class QueryCommand(val db: String, val collection: String, val filter: JsonObject, val sort: JsonObject)

class Query(doc: JsonObject) : ReadOperation(doc) {

    fun command() : QueryCommand {
        val command = when {
            doc.containsKey("command") -> doc.obj("command")!!
            doc.containsKey("query") -> doc.obj("query")!!
            else -> throw Exception("unexpected query profile document format, could not find either the [\"command\", \"query\"] field")
        }

        var sort = JsonObject()

        if (command.containsKey("sort")) {
            sort = command.obj("sort")!!
        }

        return QueryCommand(
            namespace().db,
            command.string("find")!!,
            command.obj("filter")!!,
            sort)
    }

    fun numberDeleted() = doc.int("nDeleted")
}