package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonObject
import com.mconsulting.indexrecommender.Namespace
import org.bson.BsonDocument
import org.bson.BsonString

abstract class Operation(val doc: JsonObject) {
    fun namespace() = Namespace.parse(doc.string("ns")!!)

    fun milis() = doc.int("millis")!!

    fun timestamp() = readBsonDate(doc, "ts")

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

private fun readBsonDate(doc: JsonObject, field: String): Long {
    if (doc.containsKey(field)) {
        val obj = doc.obj(field)!!

        // Do we have a date entry
        if (obj.containsKey("\$date")) {
            return readBsonLong(obj, "\$date")
        } else {
            throw Exception("the field $field is not a BsonDate")
        }
    } else {
        throw Exception("the field $field does not exist on the JsonObject")
    }
}

private fun readBsonLong(doc: JsonObject, field: String): Long {
    if (doc.containsKey(field)) {
        return when (doc[field]) {
            is JsonObject -> {
                val obj = doc.obj(field)!!

                // We found a number long
                if (!obj.containsKey("\$numberLong")) {
                    throw Exception("the field $field is not a Long or could not be converted to Long")
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
                throw Exception("the field $field is not a Long or could not be converted to Long")
            }
        }
    } else {
      throw Exception("the field $field does not exist on the JsonObject")
    }
}