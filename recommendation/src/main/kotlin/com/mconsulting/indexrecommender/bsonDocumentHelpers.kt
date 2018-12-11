package com.mconsulting.indexrecommender

import com.beust.klaxon.JsonArray
import com.beust.klaxon.JsonBase
import com.beust.klaxon.JsonObject
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.ISODateTimeFormat

val formatter: DateTimeFormatter = ISODateTimeFormat.dateTime()

fun readBsonDate(doc: JsonObject, field: String): Long {
    if (!doc.containsKey(field)) throw Exception("the field $field in [${doc.toJsonString()}] does not exist")
    val obj = doc.obj(field)!!
    // Do we have a date entry
    return when {
        obj.containsKey("\$date") && obj["\$date"] is String -> formatter.parseDateTime(obj.string("\$date")).millis
        obj.containsKey("\$date") -> readBsonLong(obj, "\$date")
        else -> throw Exception("the field $field in [${doc.toJsonString()}] is not a BsonDate")
    }
}

fun readBsonLong(doc: JsonObject, field: String): Long {
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

fun <T> getJsonArrayMaybe(fieldName: String, doc: JsonObject): JsonArray<T>? = when (doc.containsKey(fieldName)) {
    true -> doc.array(fieldName)!!
    false -> null
}

fun getJsonObject(fieldName: String, doc: JsonObject) = when(val value = getJsonObjectMaybe(fieldName, doc)) {
    null -> throw Exception("unexpected field type")
    else -> value
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

fun getLongDefault(fieldName: String, doc: Any, default: Long = 0) = when(val value = getLongMaybe(fieldName, doc)) {
    null -> default
    else -> value
}

fun getLong(fieldName: String, doc: JsonObject) = when(val value = getLongMaybe(fieldName, doc)) {
    null -> throw Exception("unexpected field type")
    else -> value
}

fun getLongMaybe(fieldName: String, doc: Any) : Long? {
    return when {
        doc is JsonObject && doc.containsKey(fieldName) -> {
            when (doc[fieldName]) {
                is Double -> doc.double(fieldName)!!.toLong()
                is Int -> doc.int(fieldName)!!.toLong()
                is Long -> doc.long(fieldName)!!
                else -> throw Exception("unexpected field type")
            }
        }
        doc is BsonDocument && doc.containsKey(fieldName) -> {
            when (doc[fieldName]) {
                is BsonDouble -> doc.getDouble(fieldName)!!.value.toLong()
                is BsonInt32 -> doc.getInt32(fieldName)!!.value.toLong()
                is BsonInt64 -> doc.getInt64(fieldName)!!.value
                else -> throw Exception("unexpected field type")
            }
        }
        else -> null
    }
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