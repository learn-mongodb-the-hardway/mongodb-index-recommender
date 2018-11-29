package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonArray
import com.beust.klaxon.JsonObject
import java.lang.Exception

data class DeleteCommand(val db: String, val collection: String, val queries: List<JsonObject>)

class Delete(doc: JsonObject) : WriteOperation(doc) {
    fun command() : DeleteCommand {
        if (doc.string("op") == "remove") {
            return removeOp(doc)
        } else if (doc.string("op") == "command") {
            val command = doc.obj("command")!!

            if (command.containsKey("deletes")) {
                return modernDelete(command.array("deletes")!!)
            }
        }

        throw Exception("cannot identify operation as a delete [${doc.toJsonString()}")
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
        return DeleteCommand(namespace().db, namespace().collection, listOf(
            doc.obj("command")!!.obj("q")!!
        ))
    }

    fun numberDeleted() = getInt("ndeleted", doc)
}