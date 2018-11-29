package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonObject
import org.bson.BsonDocument

data class UpdateCommand(val db: String, val collection: String, val query: JsonObject)

class Update(doc: JsonObject) : WriteOperation(doc) {
    fun command() : UpdateCommand {
        val command = doc.obj("command")!!

        return UpdateCommand(
            namespace().db,
            namespace().collection,
            command.obj("q")!!)
    }

    fun numberModified() = getInt("nModified", doc)
}