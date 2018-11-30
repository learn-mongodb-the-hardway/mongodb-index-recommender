package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonArray
import com.beust.klaxon.JsonObject
import java.lang.Exception

data class DeleteCommand(val db: String, val collection: String, val queries: List<JsonObject>)

class Delete(doc: JsonObject) : WriteOperation(doc) {
    fun command() : DeleteCommand {
        if (doc.string("op") == "remove" && doc.containsKey("command")) {
            return singleDeleteCommand(doc)
        } else if (doc.string("op") == "remove") {
            return removeOp(doc)
        } else if (doc.string("op") == "command") {
            val command = doc.obj("command")!!

            if (command.containsKey("deletes")) {
                return modernDelete(command.array("deletes")!!)
            }
        }

        throw Exception("cannot identify operation as a delete [${doc.toJsonString()}")
    }

    private fun singleDeleteCommand(doc: JsonObject): DeleteCommand {
        val command = doc.obj("command")!!
        val query = command.obj("q")!!

        return DeleteCommand(
            namespace().db,
            namespace().collection,
            listOf(query))
    }

    private fun modernDelete(docs: JsonArray<JsonObject>): DeleteCommand {
        val jsonObjects = docs
            .map { it.obj("q")!! }

        return DeleteCommand(
            namespace().db,
            namespace().collection,
            jsonObjects)
    }

    private fun removeOp(doc: JsonObject): DeleteCommand {
        val query = when (doc.containsKey("query")) {
            true -> doc.obj("query")!!
            else -> JsonObject()
        }

        return DeleteCommand(namespace().db, namespace().collection, listOf(
            query
        ))
    }

    fun numberDeleted() = getInt("ndeleted", doc)
}