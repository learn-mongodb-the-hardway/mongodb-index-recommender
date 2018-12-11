package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonArray
import com.beust.klaxon.JsonObject

data class AggregationCommand(val db: String, val collection: String, val pipeline: JsonArray<*>)

class Aggregation(doc: JsonObject) : ReadOperation(doc) {
    fun command() : AggregationCommand {
        val command = doc.obj("command")!!

        return AggregationCommand(
            namespace().db,
            namespace().collection,
            command.array<JsonObject>("pipeline")!!)
    }
}