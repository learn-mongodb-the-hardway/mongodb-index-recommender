package com.mconsulting.indexrecommender.profiling

import org.bson.BsonDocument

data class UpdateCommand(val db: String, val collection: String, val query: BsonDocument)

class Update(doc: BsonDocument) : WriteOperation(doc) {
    fun command() : UpdateCommand {
        val command = doc.getDocument("command")

        return UpdateCommand(
            namespace().db,
            namespace().collection,
            command.getDocument("q"))
    }

    fun numberModified() = doc.getInt32("nModified")
}