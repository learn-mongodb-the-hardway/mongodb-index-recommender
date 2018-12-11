package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonObject
import com.mconsulting.indexrecommender.getJsonObjectMaybe
import com.mconsulting.indexrecommender.getString

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