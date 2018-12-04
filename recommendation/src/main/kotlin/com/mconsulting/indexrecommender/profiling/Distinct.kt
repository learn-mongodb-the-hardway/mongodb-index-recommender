package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonObject

class Distinct(doc: JsonObject) : Operation(doc) {
    val key: String
        get() = getString("key", doc)
    val query: JsonObject
        get() = getJsonObjectMaybe("query", doc) ?: JsonObject()
    val readConcern: JsonObject?
        get() = getJsonObjectMaybe("readConcern", doc)
    val collation: JsonObject?
        get() = getJsonObjectMaybe("collation", doc)
}