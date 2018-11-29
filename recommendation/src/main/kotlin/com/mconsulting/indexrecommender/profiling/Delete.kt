package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonArray
import com.beust.klaxon.JsonObject

data class DeleteCommand(val db: String, val collection: String, val query: JsonArray<JsonObject>)

class Delete(doc: JsonObject) : WriteOperation(doc) {
    fun command() : DeleteCommand {
        if (doc.containsKey("command")) {
            return modernDelete(doc)
        } else {
            return legacyDelete(doc)
        }
    }

    private fun legacyDelete(doc: JsonObject): DeleteCommand {
        val queryObject: JsonObject

        if (doc.containsKey("query")) {
            queryObject = doc.obj("query")!!
        } else {
            queryObject = JsonObject()
        }

        return DeleteCommand(
            namespace().db,
            namespace().collection,
            JsonArray(listOf(queryObject)))
    }

    private fun modernDelete(doc: JsonObject): DeleteCommand {
        val command = doc.obj("command")!!

        return DeleteCommand(
            namespace().db,
            namespace().collection,
            command.array<JsonObject>("deletes")!!)
    }

    fun numberDeleted() = getInt("ndeleted", doc)
}