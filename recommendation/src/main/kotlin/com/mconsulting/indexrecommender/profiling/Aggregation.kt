package com.mconsulting.indexrecommender.profiling

import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonString

data class AggregationCommand(val db: String, val collection: String, val filter: BsonArray)

class Aggregation(val doc: BsonDocument) : Operation {
    fun namespace() = doc.getString("ns")

    fun milis() = doc.getInt64("millis")

    fun command() : AggregationCommand {
        val command = doc.getDocument("command")
        return AggregationCommand(
            command.getString("\$db").value,
            command.getString("aggregate").value,
            command.getArray("pipeline"))
    }

    fun keysExamined() = doc.getInt64("keysExamined")

    fun docsExamined() = doc.getInt64("docsExamined")

    fun numberReturned() = doc.getInt64("nreturned")

    fun isCollectionScan() = doc.getString("planSummary") == BsonString("COLSPAN")

    fun timestamp() = doc.getDateTime("ts")

    fun client() = doc.getString("client")

    fun appName() = doc.getString("appName")

    fun user() = doc.getString("user")

    fun responseLength() = doc.getInt64("responseLength")
}