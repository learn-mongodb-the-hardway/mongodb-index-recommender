package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonObject

class Count(doc: JsonObject) : Operation(doc) {
    val query: JsonObject
        get() = getJsonObjectMaybe("query", doc) ?: JsonObject()
    val limit: Int?
        get() = getIntMaybe("limit", doc)
    val skip: Int?
        get() = getIntMaybe("skip", doc)
    val hint: Any?
        get() = doc["hint"]
    val readConcern: JsonObject?
        get() = getJsonObjectMaybe("readConcern", doc)
}