package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.profiling.Aggregation
import com.mconsulting.indexrecommender.profiling.Operation
import com.mconsulting.indexrecommender.profiling.Query
import com.mconsulting.indexrecommender.profiling.QueryCommand
import org.bson.BsonArray
import org.bson.BsonDocument

class IndexRecommendations(val indexes: List<Index>)

enum class IndexDirection {
    ASCENDING, DESCENDING, UNKNOWN;

    fun value() : Int {
        return when (this) {
            ASCENDING -> 1
            DESCENDING -> -1
            UNKNOWN -> throw Exception("cannot convert index direction <UNKNOWN> into MongoDB direction")
        }
    }

    companion object {
        fun intValueOf(value: Int) : IndexDirection {
            return when (value) {
                -1 -> IndexDirection.DESCENDING
                1 -> IndexDirection.ASCENDING
                else -> IndexDirection.UNKNOWN
            }
        }
    }
}

data class Field(val name: String, val direction: IndexDirection)

data class Index(val fields: List<Field>)

class IndexRecommendationEngine {
    val candidateIndexes = mutableListOf<Index>()

    fun add(operation: Operation) {
        when (operation) {
            is Query -> {
                val query = operation.command()

                // Analyse the query and build candidate indexes
                if (isTopLevelQuery(query)) {
                    // Get sort document
                    val sort = query.sort

                    // Does it contain a sort statement (then use those for the extraction of direction)
                    candidateIndexes += Index(query.filter.entries.map {
                        // Establish the direction
                        var direction: IndexDirection = IndexDirection.UNKNOWN

                        // If the field is present in the sort use it
                        if (sort.containsKey(it.key)) {
                            direction = IndexDirection.intValueOf(sort.getInt32(it.key).value)
                        }

                        // Add the field with the sort direction
                        Field(it.key, direction)
                    })
                } else {

                }
            }
            is Aggregation -> {

            }
        }

    }

    private fun isTopLevelQuery(query: QueryCommand): Boolean {
        return query.filter.entries
            .map { it.value !is BsonDocument && it.value !is BsonArray }
            .fold(true) { acc, value -> acc.and(value) }
    }

    fun recommend() : IndexRecommendations {
        // Coalesce all the indexes
        val indexes = coalesce(candidateIndexes)
        // Return the index recommendations
        return IndexRecommendations(indexes)
    }

    private fun coalesce(candidateIndexes: MutableList<Index>): List<Index> {
        return candidateIndexes.toList()
    }
}