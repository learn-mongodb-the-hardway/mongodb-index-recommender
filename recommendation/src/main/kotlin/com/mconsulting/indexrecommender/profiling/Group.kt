package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonObject
import com.mconsulting.indexrecommender.getJsonObject
import com.mconsulting.indexrecommender.getJsonObjectMaybe
import com.mconsulting.indexrecommender.getString

class Group(doc: JsonObject) : Operation(doc) {
    val collection: String
        get() = getString("count", doc)
    val key: JsonObject
        get() = getJsonObject("key", doc)
    val reduce: JsonObject
        get() = getJsonObject("\$reduce", doc)
    val initial: JsonObject
        get() = getJsonObject("initial", doc)
    val keyf: JsonObject?
        get() = getJsonObjectMaybe("\$keyf", doc)
    val cond: JsonObject?
        get() = getJsonObjectMaybe("cond", doc)
    val finalize: JsonObject?
        get() = getJsonObjectMaybe("finalize", doc)
}