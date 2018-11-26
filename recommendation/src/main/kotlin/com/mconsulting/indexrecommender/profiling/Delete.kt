package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonObject

data class DeleteCommand(val db: String, val collection: String, val query: JsonObject)

class Delete(doc: JsonObject) : WriteOperation(doc) {
    fun command() : DeleteCommand {
        val command = doc.obj("command")!!

        return DeleteCommand(
            namespace().db,
            namespace().collection,
            command.obj("q")!!)
    }

    fun numberDeleted() = doc.int("ndeleted")!!
}