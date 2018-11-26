package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonObject

class Insert(doc: JsonObject) : WriteOperation(doc) {
    fun numberInserted() = doc.int("ninserted")!!
}