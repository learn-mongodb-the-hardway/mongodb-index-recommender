package com.mconsulting.indexrecommender

import com.beust.klaxon.JsonObject
import org.bson.BsonDocument

class CollectionStats(val document: BsonDocument) {
    val count : Long
        get() = getLongDefault("count", document, 0)
}