package com.mconsulting.indexrecommender.queryplans

import org.bson.BsonDocument
import org.bson.BsonString

enum class Stage {
    IXSCAN, DISTINCT_SCAN, SORT_KEY_GENERATOR
}

class QueryPlan(val document: BsonDocument) {
    private var queryPlanner: BsonDocument
    private var winningPlan: BsonDocument
    private var inputStage: BsonDocument?

    init {
        if(!document.containsKey("queryPlanner")) throw Exception("no queryPlanner field found")
        queryPlanner = document.getDocument("queryPlanner")

        if(!queryPlanner.containsKey("winningPlan")) throw Exception("no winningPlan field found")
        winningPlan = queryPlanner.getDocument("winningPlan")

        // Get the input stage
        inputStage = findIndexScan(winningPlan)
    }

    fun isIndexScan() : Boolean {
        if (inputStage == null) return false
        return true
    }

    private fun findIndexScan(inputStage: BsonDocument): BsonDocument? {
        var current = inputStage

        while (true) {
            if (current.containsKey("inputStage")) {
                current = current.getDocument("inputStage")

                if (current.containsKey("stage")
                    && current.getString("stage").value in listOf(Stage.IXSCAN.name, Stage.DISTINCT_SCAN.name)) {
                    return current
                }
            } else {
                break
            }
        }

        return null
    }

    fun indexName() : String? {
        if (isIndexScan()) {
            return inputStage!!.getString("indexName").value
        }

        return null
    }

    fun isUnique() : Boolean {
        if (isIndexScan()) {
            return inputStage!!.getBoolean("isUnique").value
        }

        return false
    }

    fun isSparse() : Boolean {
        if (isIndexScan()) {
            return inputStage!!.getBoolean("isSparse").value
        }

        return false
    }

    fun isPartial() : Boolean {
        if (isIndexScan()) {
            return inputStage!!.getBoolean("isPartial").value
        }

        return false
    }

    fun isMultiKey() : Boolean {
        if (isIndexScan()) {
            return inputStage!!.getBoolean("isMultiKey").value
        }

        return false
    }

    fun key() : BsonDocument? {
        if (isIndexScan()) {
            return inputStage!!.getDocument("keyPattern")
        }

        return null
    }
}