package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.indexes.Field
import com.mconsulting.indexrecommender.indexes.Index
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.indexes.SingleFieldIndex
import com.mconsulting.indexrecommender.profiling.Aggregation
import com.mconsulting.indexrecommender.profiling.Operation
import com.mconsulting.indexrecommender.profiling.Query
import com.mconsulting.indexrecommender.profiling.QueryCommand
import org.bson.BsonArray
import org.bson.BsonDocument

class IndexRecommendations(val indexes: List<Index>)

class IndexRecommendationEngine {
    val candidateIndexes = mutableListOf<Index>()

    fun add(operation: Operation) {
        when (operation) {
            is Query -> {
                val query = operation.command()

//                // Analyse the query and build candidate indexes
//                if (isTopLevelQuery(query)) {
                    // Get sort document
                    val sort = query.sort

                    // Check what kind of filter it is
                    if (query.filter.entries.size == 1) {
                        addSingleFieldIndex(query, candidateIndexes)
                    } else if (query.filter.entries.size > 1) {
                        addCompoundFieldIndex(query, candidateIndexes)
                    }

//                    // Does it contain a sort statement (then use those for the extraction of direction)
//                    candidateIndexes += Index(query.filter.entries.map {
//                        // Establish the direction
//                        var direction: IndexDirection = IndexDirection.UNKNOWN
//
//                        // If the field is present in the sort use it
//                        if (sort.containsKey(it.key)) {
//                            direction = IndexDirection.intValueOf(sort.getInt32(it.key).value)
//                        }
//
//                        // Add the field with the sort direction
//                        Field(it.key, direction)
//                    })
//                } else {
//
//                }
            }
            is Aggregation -> {

            }
        }

    }

    private fun addCompoundFieldIndex(query: QueryCommand, candidateIndexes: MutableList<Index>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    private fun addSingleFieldIndex(query: QueryCommand, candidateIndexes: MutableList<Index>) {
        // Get the first entry
        val entry = query.filter.entries.first()
        val fieldName = createIndexName(query)
        // Create the index entry
        val index = SingleFieldIndex(
            fieldName,
            Field(entry.key, getIndexDirection(query, fieldName, IndexDirection.UNKNOWN)))

        // Check if the key exists
        if (!candidateIndexes.contains(index)) {
            candidateIndexes += index
        }
    }

    private fun getIndexDirection(query: QueryCommand, fieldName: String, defaultDirection: IndexDirection = IndexDirection.ASCENDING): IndexDirection {
        var direction = defaultDirection

        if (query.sort.containsKey(fieldName)) {
            direction = IndexDirection.intValueOf(query.sort.getInt32(fieldName).value)
        }

        return direction
    }

    private fun createIndexName(query: QueryCommand): String {
        return query.filter.entries.map {
            "${it.key}_${getIndexDirection(query, it.key).value()}"
        }.joinToString("_")
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