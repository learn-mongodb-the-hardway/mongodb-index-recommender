package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonArray
import com.beust.klaxon.JsonObject
import java.lang.Exception

data class UpdateCommand(val db: String, val collection: String, val queries: List<JsonObject>) {
    fun toQueryCommands(): List<QueryCommand> {
        return queries.map {
            QueryCommand(db, collection, it, JsonObject())
        }
    }
}

class Update(doc: JsonObject) : WriteOperation(doc) {
    fun command() : UpdateCommand {
        if (doc.string("op") == "update" && doc.containsKey("command")) {
            return singleUpdateCommand(doc)
        } else if (doc.string("op") == "update") {
            return updateOp(doc)
        } else if (doc.string("op") == "command") {
            val command = doc.obj("command")!!

            if (command.containsKey("updates")) {
                return modernUpdate(command.array("updates")!!)
            }
        }

        throw Exception("cannot identify operation as a update [${doc.toJsonString()}")
    }

    private fun singleUpdateCommand(doc: JsonObject): UpdateCommand {
        val command = doc.obj("command")!!
        val query = command.obj("q")!!

        return UpdateCommand(
            namespace().db,
            namespace().collection,
            listOf(query))
    }

    private fun modernUpdate(docs: JsonArray<JsonObject>): UpdateCommand {
        val jsonObjects = docs
            .map { it.obj("q")!! }

        return UpdateCommand(
            namespace().db,
            namespace().collection,
            jsonObjects)
    }

    private fun updateOp(doc: JsonObject): UpdateCommand {
        val query = when (doc.containsKey("query")) {
            true -> doc.obj("query")!!
            else -> JsonObject()
        }

        return UpdateCommand(namespace().db, namespace().collection, listOf(
            query
        ))
    }

    fun numberModified() = getInt("nModified", doc)
}