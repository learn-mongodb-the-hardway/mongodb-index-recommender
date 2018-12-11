package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonBase
import com.beust.klaxon.JsonObject
import com.mconsulting.indexrecommender.getBoolean
import com.mconsulting.indexrecommender.getBooleanMaybe
import com.mconsulting.indexrecommender.getDoubleMaybe
import com.mconsulting.indexrecommender.getIntMaybe
import com.mconsulting.indexrecommender.getJsonBase
import com.mconsulting.indexrecommender.getJsonObjectMaybe
import com.mconsulting.indexrecommender.getStringMaybe

// {"geoNear":"geo_near_random2","near":[-65.1009848893434,-36.22633492201567],"num":48.0,"spherical":1.0}
class GeoNear(doc: JsonObject) : Operation(doc) {
    val near: JsonBase
        get() = getJsonBase("near", doc)
    val spherical: Boolean
        get() = getBoolean("spherical", doc)
    val limit: Int?
        get() = getIntMaybe("limit", doc)
    val num: Int?
        get() = getIntMaybe("num", doc)
    val minDistance: Int?
        get() = getIntMaybe("minDistance", doc)
    val maxDistance: Int?
        get() = getIntMaybe("maxDistance", doc)
    val query: JsonObject?
        get() = getJsonObjectMaybe("query", doc)
    val distanceMultiplier: Double?
        get() = getDoubleMaybe("distanceMultiplier", doc)
    val includeLocs: Boolean?
        get() = getBooleanMaybe("includeLocs", doc)
    val uniqueDocs: Boolean?
        get() = getBooleanMaybe("uniqueDocs", doc)
    val readConcern: JsonObject?
        get() = getJsonObjectMaybe("readConcern", doc)
    val key: String?
        get() = getStringMaybe("key", doc)
}