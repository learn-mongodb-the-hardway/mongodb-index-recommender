package com.mconsulting.indexrecommender.profiling

import com.mconsulting.indexrecommender.Namespace
import org.bson.BsonDocument
import org.bson.BsonString

abstract class Operation(val doc: BsonDocument) {
    fun namespace() = Namespace.parse(doc.getString("ns").value)

    fun milis() = doc.getInt32("millis")

    fun timestamp() = doc.getDateTime("ts")

    fun client() = doc.getString("client")

    fun appName() = doc.getString("appName")

    fun user() = doc.getString("user")
}

abstract class ReadOperation(doc: BsonDocument) : Operation(doc) {
    fun keysExamined() = doc.getInt32("keysExamined")

    fun docsExamined() = doc.getInt32("docsExamined")

    fun numberReturned() = doc.getInt32("nreturned")

    fun isCollectionScan() = doc.getString("planSummary") == BsonString("COLSPAN")

    fun responseLength() = doc.getInt32("responseLength")
}

abstract class WriteOperation(doc: BsonDocument) : Operation(doc)