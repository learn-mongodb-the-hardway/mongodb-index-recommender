package com.mconsulting.indexrecommender.indexes

import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.json.JsonWriterSettings

abstract class Index(val sparse: Boolean = false, val unique: Boolean = false)

enum class IndexDirection {
    ASCENDING, DESCENDING, UNKNOWN;

    fun value() : Int {
        return when (this) {
            ASCENDING -> 1
            DESCENDING -> -1
            UNKNOWN -> throw Exception("cannot convert index direction <UNKNOWN> into MongoDB direction")
        }
    }

    companion object {
        fun intValueOf(value: Int) : IndexDirection {
            return when (value) {
                -1 -> IndexDirection.DESCENDING
                1 -> IndexDirection.ASCENDING
                else -> IndexDirection.UNKNOWN
            }
        }
    }
}

data class Field(val name: String, val direction: IndexDirection)

class IdIndex : Index(unique = true)

class SingleFieldIndex(val field: Field, sparse: Boolean = false, unique: Boolean = false) : Index(sparse, unique)

class CompoundIndex(val fields: List<Field>, sparse: Boolean = false, unique: Boolean = false) : Index(sparse, unique)

class MultikeyIndex : Index()

abstract class GeospatialIndex : Index()

class TwoDSphereIndex : GeospatialIndex()

class TwoDIndex: GeospatialIndex()

class TextIndex : Index()

class HashedIndex : Index()

class PartialIndex : Index()

class TTLIndex : Index()

fun createIndex(document: BsonDocument) : Index {
    println(document.toJson(JsonWriterSettings.builder().indent(true).build()))

    if (document.containsKey("key")) {
        val key = document.getDocument("key")

        if (key.size == 1 && key.containsKey("_id")) {
            return IdIndex()
        } else if (key.size == 1) {
            return SingleFieldIndex(Field(
                key.firstKey,
                IndexDirection.intValueOf(key.getInt32(key.firstKey).value)))
        } else if (key.size > 1) {
            // TODO: Check if we have multikey indexes (we need to query using this index and look at the explain plan)
            // to detect if it's a multikey index

            return CompoundIndex(key.entries.map { Field(it.key, IndexDirection.intValueOf((it.value as BsonInt32).value)) })
        }
    }

    return TTLIndex()
}