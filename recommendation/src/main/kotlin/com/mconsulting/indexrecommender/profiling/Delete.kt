package com.mconsulting.indexrecommender.profiling

import org.bson.BsonDocument

data class DeleteCommand(val db: String, val collection: String, val query: BsonDocument)

class Delete(doc: BsonDocument) : WriteOperation(doc) {
    fun command() : DeleteCommand {
        val command = doc.getDocument("command")

        return DeleteCommand(
            namespace().db,
            namespace().collection,
            command.getDocument("q"))
    }

    fun numberDeleted() = doc.getInt32("ndeleted")
}