package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonObject
import com.mconsulting.indexrecommender.getInt

class Insert(doc: JsonObject) : WriteOperation(doc) {
    fun numberInserted() = getInt("ninserted", doc)
}