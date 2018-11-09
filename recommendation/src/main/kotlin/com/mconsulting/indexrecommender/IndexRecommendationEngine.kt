package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.indexes.CompoundIndex
import com.mconsulting.indexrecommender.indexes.Field
import com.mconsulting.indexrecommender.indexes.Index
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.indexes.MultikeyIndex
import com.mconsulting.indexrecommender.indexes.SingleFieldIndex
import com.mconsulting.indexrecommender.profiling.Aggregation
import com.mconsulting.indexrecommender.profiling.Operation
import com.mconsulting.indexrecommender.profiling.Query
import com.mconsulting.indexrecommender.profiling.QueryCommand
import com.mongodb.MongoClient
import org.bson.BsonDocument

data class IndexRecommendationOptions(val executeQueries: Boolean = true)

class IndexRecommendationEngine(
    val client: MongoClient,
    val options: IndexRecommendationOptions = IndexRecommendationOptions()) {

    val candidateIndexes = mutableListOf<Index>()

    fun add(operation: Operation) {
        when (operation) {
            is Query -> {
                val query = operation.command()

                // Check if we have a $geoIntersects or $geoWithin query
                if (isGeoQueryIndex(query)) {
                    return addGeoQueryIndex(query, candidateIndexes)
                }

                // Check if w have a multikey index
                if (isMultiKeyIndex(query)) {
                    return addMultiKeyIndex(query, candidateIndexes)
                }

                // Check if it's a single field index
                if (query.filter.entries.size == 1) {
                    addSingleFieldIndex(query, candidateIndexes)
                }

                // Check if it's a compound index
                if (query.filter.entries.size > 1) {
                    addCompoundFieldIndex(query, candidateIndexes)
                }
            }
            is Aggregation -> {
            }
        }

    }

    private fun addGeoQueryIndex(query: QueryCommand, candidateIndexes: MutableList<Index>) {

    }

    private fun isGeoQueryIndex(query: QueryCommand): Boolean {
        return containsGeoSpatialPredicate(query.filter) { doc, parent, entry ->
            if (entry.key in listOf("\$geoWithin", "\$near", "\$geoIntersects", "\$nearSphere")) {
                true
            } else {
                false
            }
        }
    }

    fun add(index: Index) {
        if (!candidateIndexes.contains(index)) {
            candidateIndexes += index
        }
    }

    fun recommend() : List<Index> {
        return coalesce(candidateIndexes)
    }

    private fun isMultiKeyIndex(query: QueryCommand) : Boolean {
        // Find the document specified
        val doc = client
            .getDatabase(query.db)
            .getCollection(query.collection, BsonDocument::class.java)
            .find(query.filter)
            .projection(generateProjection(query.filter))
            .firstOrNull()

        // Establish the types for each field to check if we are multikey or not
        doc ?: return false

        // If one of the fields in the document is an array we are dealing with a candidate multikey index
        // ex: { games: [1, 2, 3] } or { games: [{ id: 1 }] }
        if (containsArray(doc)) {
            return true
        }

        // Not a multikey index
        return false
    }

    private fun addMultiKeyIndex(query: QueryCommand, candidateIndexes: MutableList<Index>) {
        // Create list of fields
        val fields = query.filter.entries.map {
            Field(it.key, getIndexDirection(query, it.key, IndexDirection.UNKNOWN))
        }

        // Create index name
        val fieldName = createIndexName(query)

        // Create Multikey index
        val index = MultikeyIndex(fieldName, fields)

        // Add to candidate list if it does not already exist in it
        if (!candidateIndexes.contains(index)) {
            candidateIndexes += index
        }
    }

    private fun addCompoundFieldIndex(query: QueryCommand, candidateIndexes: MutableList<Index>) {
        // Create list of fields
        val fields = query.filter.entries.map {
            Field(it.key, getIndexDirection(query, it.key, IndexDirection.UNKNOWN))
        }

        // Create index name
        val fieldName = createIndexName(query)

        // Create compound index
        val index = CompoundIndex(fieldName, fields)

        // Add to candidate list if it does not already exist in it
        if (!candidateIndexes.contains(index)) {
            candidateIndexes += index
        }
    }

    private fun addSingleFieldIndex(query: QueryCommand, candidateIndexes: MutableList<Index>) {
        // Get the first entry
        val entry = query.filter.entries.first()
        val fieldName = createIndexName(query)

        // Create the index entry
        val index = SingleFieldIndex(
            fieldName,
            Field(entry.key, getIndexDirection(query, entry.key, IndexDirection.UNKNOWN)))

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

    private fun coalesce(candidateIndexes: MutableList<Index>): List<Index> {
        return candidateIndexes.toList()
    }
}