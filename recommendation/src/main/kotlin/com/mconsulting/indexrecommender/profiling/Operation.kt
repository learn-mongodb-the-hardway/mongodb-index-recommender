package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonBase
import com.beust.klaxon.JsonObject
import com.mconsulting.indexrecommender.Namespace
import org.bson.BsonDocument
import org.bson.BsonString
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.ISODateTimeFormat

abstract class Operation(val doc: JsonObject) {
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

val formatter: DateTimeFormatter = ISODateTimeFormat.dateTime()

private fun readBsonDate(doc: JsonObject, field: String): Long {
    if (!doc.containsKey(field)) throw Exception("the field $field in [${doc.toJsonString()}] does not exist")
    val obj = doc.obj(field)!!
    // Do we have a date entry
    return when {
        obj.containsKey("\$date") && obj["\$date"] is String -> formatter.parseDateTime(obj.string("\$date")).millis
        obj.containsKey("\$date") -> readBsonLong(obj, "\$date")
        else -> throw Exception("the field $field in [${doc.toJsonString()}] is not a BsonDate")
    }
}

private fun readBsonLong(doc: JsonObject, field: String): Long {
    if (doc.containsKey(field)) {
        return when (doc[field]) {
            is JsonObject -> {
                val obj = doc.obj(field)!!

                // We found a number long
                if (!obj.containsKey("\$numberLong")) {
                    throw Exception("the field $field in [${doc.toJsonString()}] is not a Long or could not be converted to Long")
                }

                obj.string("\$numberLong")!!.toLong()
            }
            is Int -> {
                doc.int(field)!!.toLong()
            }
            is Long -> {
                doc.long(field)!!
            }
            else -> {
                throw Exception("the field $field in [${doc.toJsonString()}] is not a Long or could not be converted to Long")
            }
        }
    } else {
      throw Exception("the field $field in [${doc.toJsonString()}] does not exist on the JsonObject")
    }
}

fun getJsonObjectMaybe(fieldName: String, doc: JsonObject) = when (doc.containsKey(fieldName)) {
    true -> doc.obj(fieldName)!!
    false -> null
}

fun getStringMaybe(fieldName: String, doc: JsonObject) = when (doc.containsKey(fieldName)) {
    true -> doc.string(fieldName)!!
    false -> null
}

fun getString(fieldName: String, doc: JsonObject) = when(val value = getStringMaybe(fieldName, doc)) {
    null -> throw Exception("unexpected field type")
    else -> value
}

fun getJsonBase(fieldName: String, doc: JsonObject) = doc[fieldName]!! as JsonBase

fun getInt(fieldName: String, doc: JsonObject) = when(val value = getIntMaybe(fieldName, doc)) {
    null -> throw Exception("unexpected field type")
    else -> value
}

fun getIntMaybe(fieldName: String, doc: JsonObject) = when (doc.containsKey(fieldName)) {
    true -> {
        when (doc[fieldName]) {
            is Double -> doc.double(fieldName)!!.toInt()
            is Int -> doc.int(fieldName)!!
            is Long -> doc.long(fieldName)!!.toInt()
            else -> throw Exception("unexpected field type")
        }
    }
    false -> null
}

fun getDoubleMaybe(fieldName: String, doc: JsonObject) = when (doc.containsKey(fieldName)) {
    true -> {
        when (doc[fieldName]) {
            is Double -> doc.double(fieldName)!!
            is Int -> doc.int(fieldName)!!.toDouble()
            is Long -> doc.long(fieldName)!!.toDouble()
            else -> throw Exception("unexpected field type")
        }
    }
    false -> null
}

fun getBoolean(fieldName: String, doc: JsonObject) = when(val value = getBooleanMaybe(fieldName, doc)) {
    null -> throw Exception("unexpected field type")
    else -> value
}

fun getBooleanMaybe(fieldName: String, doc: JsonObject) = when (doc[fieldName]) {
    is Boolean -> doc[fieldName] as Boolean
    is Double -> doc.double(fieldName) == 1.0
    is Int -> doc.int(fieldName) == 1
    else -> null
}