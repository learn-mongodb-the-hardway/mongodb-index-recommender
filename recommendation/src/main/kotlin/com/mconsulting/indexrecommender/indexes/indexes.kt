package com.mconsulting.indexrecommender.indexes

import org.bson.BsonDocument
import org.bson.json.JsonWriterSettings

abstract class Index(val sparse: Boolean = false, val unique: Boolean = false)

class IdIndex : Index()

class SingleFieldIndex : Index()

class CompoundIndex : Index()

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
        }
    }

    return TTLIndex()
}