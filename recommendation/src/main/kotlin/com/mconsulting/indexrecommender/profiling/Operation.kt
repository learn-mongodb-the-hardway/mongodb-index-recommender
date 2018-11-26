package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonObject
import com.mconsulting.indexrecommender.Namespace
import org.bson.BsonDocument
import org.bson.BsonString

abstract class Operation(val doc: JsonObject) {
    fun namespace() = Namespace.parse(doc.string("ns")!!)

    fun milis() = doc.int("millis")!!

    fun timestamp() = doc.string("ts")!!

    fun client() = doc.string("client")!!

    fun appName() = doc.string("appName")!!

    fun user() = doc.string("user")!!
}

abstract class ReadOperation(doc: JsonObject) : Operation(doc) {
    fun keysExamined() = doc.int("keysExamined")!!

    fun docsExamined() = doc.int("docsExamined")!!

    fun numberReturned() = doc.int("nreturned")!!

    fun isCollectionScan() = doc.string("planSummary") == "COLSPAN"

    fun responseLength() = doc.int("responseLength")!!
}

abstract class WriteOperation(doc: JsonObject) : Operation(doc)