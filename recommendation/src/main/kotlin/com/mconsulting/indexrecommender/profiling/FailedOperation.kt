package com.mconsulting.indexrecommender.profiling

import com.beust.klaxon.JsonObject
import com.mconsulting.indexrecommender.getInt
import com.mconsulting.indexrecommender.getString

class FailedOperation(doc: JsonObject) : Operation(doc) {

    fun exception() = {
        if (doc.containsKey("exception")) {
            getString("exception", doc)
        } else {
            getString("errName", doc)
        }
    }

    fun exceptionCode() = {
        if (doc.containsKey("exceptionCode")) {
            getInt("exceptionCode", doc)
        } else {
            getInt("errCode", doc)
        }
    }

}