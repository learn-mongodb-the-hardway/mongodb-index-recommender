package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.indexes.CompoundIndex
import com.mconsulting.indexrecommender.indexes.Field
import com.mconsulting.indexrecommender.indexes.Index
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.indexes.MultikeyIndex
import com.mconsulting.indexrecommender.indexes.SingleFieldIndex
import com.mconsulting.indexrecommender.indexes.TextField
import com.mconsulting.indexrecommender.indexes.TextIndex
import com.mconsulting.indexrecommender.indexes.TwoDSphereIndex
import com.mconsulting.indexrecommender.profiling.Aggregation
import com.mconsulting.indexrecommender.profiling.Operation
import com.mconsulting.indexrecommender.profiling.Query
import com.mconsulting.indexrecommender.profiling.QueryCommand
import com.mongodb.MongoClient
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonElement
import org.bson.BsonInt32
import org.bson.BsonRegularExpression
import org.bson.BsonValue

data class IndexRecommendationOptions(val executeQueries: Boolean = true)

fun BsonDocument.remove(path: List<String>) {
    var pointer: BsonValue? = this
    var previousPointer: BsonValue? = this

    for(entry in path) {
        previousPointer = pointer
        pointer = when (pointer) {
            is BsonDocument -> pointer.get(entry)
            is BsonArray -> pointer[entry.toInt()]
            else -> pointer
        }
    }

    when (previousPointer) {
        is BsonDocument -> previousPointer.remove(path.last())
        is BsonArray -> previousPointer.removeAt(path.last().toInt())
    }
}

class IndexRecommendationEngine(
    val client: MongoClient,
    val options: IndexRecommendationOptions = IndexRecommendationOptions()) {

    val candidateIndexes = mutableListOf<Index>()

    fun add(operation: Operation) {
        when (operation) {
            is Query -> processQuery(operation.command())
            is Aggregation -> {
            }
        }

    }

    private fun processQuery(query: QueryCommand) {
        // Check if we have a $geoIntersects or $geoWithin query
        if (isGeoQueryIndex(query)) {
            return addGeoQueryIndex(query, candidateIndexes)
        }

        // Do we have query that contains a regular expression
        if (containsRegularExpression(query)) {
            return processRegularExpression(query, candidateIndexes)
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

    /**
     * We will specify a possible text index for each regular expression entry
     * The left over fields will then be processed against other possible index candidates
     */
    private fun processRegularExpression(query: QueryCommand, candidateIndexes: MutableList<Index>) {
        val regularExpressions = mutableListOf<List<String>>()
        val filteredOutDoc = query.filter.clone()
        val filteredSortDoc = query.sort.clone()

        traverse(query.filter) { doc, _path, entry ->
            if (entry.value is BsonRegularExpression) {
                // Add to the list of regular expressions
                regularExpressions.add(_path.toList() + entry.key)
                // Remove from the filtered out Doc
                filteredOutDoc.remove(_path + entry.key)
                // Remove from the sort
                filteredSortDoc.remove(_path + entry.key)
            }
        }

        // Generate a text index recommendation
        candidateIndexes += TextIndex(
            createIndexName(QueryCommand(query.db, query.collection, BsonDocument(regularExpressions.map {
                BsonElement(it.joinToString("."), BsonInt32(1))
            }), BsonDocument())), regularExpressions.map {
                TextField(it)
            })

        // Process the rest of the filter as a possible other index
        if (filteredOutDoc.isNotEmpty()) {
            processQuery(QueryCommand(query.db, query.collection, filteredOutDoc, filteredSortDoc))
        }
    }

    private fun containsRegularExpression(query: QueryCommand): Boolean {
        var contains = false

        traverse(query.filter) { doc, _path, entry ->
            if (entry.value is BsonRegularExpression) contains = true
        }

        return contains
    }

    private fun addGeoQueryIndex(query: QueryCommand, candidateIndexes: MutableList<Index>) {
        var path: MutableList<String> = mutableListOf()

        traverse(query.filter) { doc, _path, entry ->
            if (entry.key in listOf("\$geoWithin", "\$near", "\$geoIntersects", "\$nearSphere")) {
               path.addAll(_path)
            }
        }

        // If we have a path, we have a geo index candidate
        if (path.isNotEmpty()) {
            candidateIndexes += TwoDSphereIndex(createIndexName(query), path.joinToString("."))
        }
   }

    private fun isGeoQueryIndex(query: QueryCommand): Boolean {
        var contains = false

        traverse(query.filter) { doc, _path, entry ->
            if (entry.key in listOf("\$geoWithin", "\$near", "\$geoIntersects", "\$nearSphere")) {
                contains = true
            }
        }

        return contains
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