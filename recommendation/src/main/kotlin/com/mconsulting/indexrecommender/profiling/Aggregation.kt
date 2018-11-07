package com.mconsulting.indexrecommender.profiling

import org.bson.BsonArray
import org.bson.BsonDocument

data class AggregationCommand(val db: String, val collection: String, val pipeline: BsonArray)

class Aggregation(doc: BsonDocument) : ReadOperation(doc) {
    fun command() : AggregationCommand {
        val command = doc.getDocument("command")
        return AggregationCommand(
            namespace().db,
            command.getString("aggregate").value,
            command.getArray("pipeline"))
    }
}