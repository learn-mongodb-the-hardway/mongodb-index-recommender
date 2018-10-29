package com.mconsulting.indexrecommender.profiling

import org.bson.BsonDocument
import org.bson.BsonString

data class QueryCommand(val db: String, val collection: String, val filter: BsonDocument, val sort: BsonDocument)

class Query(val doc: BsonDocument) : Operation {
    fun namespace() = doc.getString("ns")

    fun milis() = doc.getInt64("millis")

    fun command() : QueryCommand {
        val command = doc.getDocument("command")
        var sort: BsonDocument = BsonDocument()

        if (command.containsKey("sort")) {
            sort = command.getDocument("sort")
        }

        return QueryCommand(
            command.getString("\$db").value,
            command.getString("find").value,
            command.getDocument("filter"),
            sort)
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