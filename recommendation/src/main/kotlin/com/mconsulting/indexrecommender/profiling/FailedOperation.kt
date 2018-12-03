package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonObject

class FailedOperation(doc: JsonObject) : Operation(doc) {

    fun exception() = getString("exception", doc)

    fun exceptionCode() = getInt("exceptionCode", doc)

}