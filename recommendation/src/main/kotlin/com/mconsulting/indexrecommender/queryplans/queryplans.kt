package com.mconsulting.indexrecommender.queryplans

import org.bson.BsonDocument
import org.bson.BsonString

enum class Stage {
    IXSCAN, DISTINCT_SCAN
}

class QueryPlan(val document: BsonDocument) {
    private val queryPlanner: BsonDocument
    private val winningPlan: BsonDocument
    private val inputStage: BsonDocument
    private var inputStageStage: BsonString

    init {
        if(!document.containsKey("queryPlanner")) throw Exception("no queryPlanner field found")
        queryPlanner = document.getDocument("queryPlanner")

        if(!queryPlanner.containsKey("winningPlan")) throw Exception("no winningPlan field found")
        winningPlan = queryPlanner.getDocument("winningPlan")

        if(!winningPlan.containsKey("inputStage")) throw Exception("no inputStage field found")
        inputStage = winningPlan.getDocument("inputStage")

        // Get inputStage name
        inputStageStage = inputStage.getString("stage")
    }

    fun isIndexScan() : Boolean {
        if (inputStageStage.value in listOf(Stage.IXSCAN.name, Stage.DISTINCT_SCAN.name)) {
            return true
        }

        return false
    }

    fun indexName() : String? {
        if (isIndexScan()) {
            return inputStage.getString("indexName").value
        }

        return null
    }

    fun isUnique() : Boolean {
        if (isIndexScan()) {
            return inputStage.getBoolean("isUnique").value
        }

        return false
    }

    fun isSparse() : Boolean {
        if (isIndexScan()) {
            return inputStage.getBoolean("isSparse").value
        }

        return false
    }

    fun isPartial() : Boolean {
        if (isIndexScan()) {
            return inputStage.getBoolean("isPartial").value
        }

        return false
    }

    fun isMultiKey() : Boolean {
        if (isIndexScan()) {
            return inputStage.getBoolean("isMultiKey").value
        }

        return false
    }

    fun key() : BsonDocument? {
        if (isIndexScan()) {
            return inputStage.getDocument("keyPattern")
        }

        return null
    }
}