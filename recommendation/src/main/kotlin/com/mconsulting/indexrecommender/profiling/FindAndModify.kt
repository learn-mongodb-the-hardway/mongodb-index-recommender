package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonArray
import com.beust.klaxon.JsonObject

class FindAndModify(doc: JsonObject) : Operation(doc) {
    val query: JsonObject
        get() = getJsonObjectMaybe("query", doc) ?: JsonObject()
    val sort: JsonObject
        get() = getJsonObjectMaybe("sort", doc) ?: JsonObject()
    val remove: Boolean
        get() = getBooleanMaybe("remove", doc) ?: false
    val update: JsonObject?
        get() = getJsonObject("update", doc)
    val new: Boolean
        get() = getBooleanMaybe("new", doc) ?: false
    val fields: JsonObject?
        get() = getJsonObject("fields", doc)
    val upsert: Boolean
        get() = getBooleanMaybe("upsert", doc) ?: false
    val bypassDocumentValidation: Boolean
        get() = getBooleanMaybe("bypassDocumentValidation", doc) ?: false
    val writeConcern: JsonObject?
        get() = getJsonObjectMaybe("writeConcern", doc)
    val maxTimeMS: Int?
        get() = getIntMaybe("maxTimeMS", doc)
    val collation: JsonObject?
        get() = getJsonObjectMaybe("collation", doc)
    val arrayFilters: JsonArray<*>?
        get() = getJsonArrayMaybe<JsonObject>("arrayFilters", doc)
}