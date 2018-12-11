package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonArray
import com.beust.klaxon.JsonBase
import com.beust.klaxon.JsonObject
import com.mconsulting.indexrecommender.Namespace
import com.mconsulting.indexrecommender.getInt
import com.mconsulting.indexrecommender.getString
import com.mconsulting.indexrecommender.readBsonDate
import org.bson.BsonDocument
import org.bson.BsonString
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.ISODateTimeFormat

abstract class Operation(val doc: JsonObject) {
    fun containsNameSpace() = doc.containsKey("ns")

    fun namespace() = Namespace.parse(doc.string("ns")!!)

    fun millis() = getInt("millis", doc)

    fun timestamp() = readBsonDate(doc, "ts")

    fun client() = getString("client", doc)

    fun appName() = getString("appName", doc)

    fun user() = getString("user", doc)
}

abstract class ReadOperation(doc: JsonObject) : Operation(doc) {
    fun keysExamined() = getInt("keysExamined", doc)

    fun docsExamined() = getInt("docsExamined", doc)

    fun numberReturned() = getInt("nreturned", doc)

    fun isCollectionScan() = getString("planSummary", doc) == "COLSPAN"

    fun responseLength() = getInt("responseLength", doc)
}

abstract class WriteOperation(doc: JsonObject) : Operation(doc)