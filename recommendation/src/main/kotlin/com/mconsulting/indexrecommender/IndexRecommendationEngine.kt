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
import com.mconsulting.indexrecommender.log.LogEntry
import com.mconsulting.indexrecommender.profiling.Aggregation
import com.mconsulting.indexrecommender.profiling.AggregationCommand
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
    val collection: Collection? = null,
    val options: IndexRecommendationOptions = IndexRecommendationOptions()) {

    val candidateIndexes = mutableListOf<Index>()

    fun process(operation: Operation) {
        when (operation) {
            is Query -> processQuery(operation)
            is Aggregation -> processAggregation(operation)
        }
    }

    fun process(logEntry: LogEntry) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    private fun processAggregation(aggregation: Aggregation) {
        val indexes = mutableListOf<Index>()
        val aggregationCommand = aggregation.command()

        // Identify any lookup fields
        aggregationCommand
            .pipeline
            .filterIsInstance<BsonDocument>()
            .forEach {

                // Do we have a $lookup stage
                if (it.containsKey("\$lookup")) {
                    addLookupIndex(indexes, aggregation, it)
                }
        }

        // For each index add it to the candidate indexes
        indexes.forEach {
            if (!candidateIndexes.contains(it)) {
                candidateIndexes += it
            }
        }
    }

    private fun addLookupIndex(indexes: MutableList<Index>, aggregation: Aggregation, document: BsonDocument) {
        val lookupDocument = document.getDocument("\$lookup")

        if (lookupDocument.containsKey("localField")) {
            processBasicLookup(lookupDocument, aggregation, indexes)
        } else if (lookupDocument.containsKey("pipeline")) {
            processAdvancedLookup(lookupDocument, aggregation, indexes)
        }
    }

    private fun processAdvancedLookup(document: BsonDocument, aggregation: Aggregation, indexes: MutableList<Index>) {
        // Unpack the fields
        val from = document.getString("from").value
        val pipeline = document.getArray("pipeline")

        // Locate any $match expression
        val matchExpression = extractMatchStatement(pipeline)

        // We need to lookup the other collection and add the index on it
        // Get the collection
        val collection = collection!!.db.getCollection(Namespace(aggregation.namespace().db, from))

        // Process the match expression using the same code path as a QUERY
        if (!matchExpression.containsKey("\$expr")) {
            val indexes = processQueryCommand(QueryCommand(aggregation.namespace().db, from, matchExpression, BsonDocument()))

            indexes.forEach {
                collection.addIndex(it)
            }

            return
        }

        // Contain the index
        var index: Index
        val extractFieldNames = extractFieldNamesFromMatchExpression(matchExpression)

        // Create the index
        if (extractFieldNames.size == 1) {
            index = SingleFieldIndex("${extractFieldNames[0]}_1", Field(extractFieldNames[0], IndexDirection.UNKNOWN))
        } else {
            index = CompoundIndex(
                extractFieldNames.map { "${it}_1" }.joinToString("_"),
                extractFieldNames.map { Field(it, IndexDirection.UNKNOWN) })
        }

        // Add the operation to the right collection
        collection.addIndex(index)
    }

    private fun extractFieldNamesFromMatchExpression(matchExpression: BsonDocument): List<String> {
        val fieldNames = mutableListOf<String>()

        traverse(matchExpression) { doc, path, entry ->
            // Do we have an $expr for the comparison
            val value = entry.value
            if (value is BsonArray) {
                when (entry.key) {
                    "\$eq",
                    "\$gt",
                    "\$gte",
                    "\$lt",
                    "\$lte",
                    "\$cmp",
                    "\$ne" -> {
                        var fieldName = (entry.value as BsonArray).get(0).asString().value
                        if (fieldName.startsWith("\$")) {
                            fieldName = fieldName.substring(1)
                        }

                        fieldNames += fieldName
                    }
                }
            }
        }

        return fieldNames
    }

    private fun extractMatchStatement(pipeline: BsonArray): BsonDocument {
        return pipeline.filterIsInstance<BsonDocument>().first {
            it.containsKey("\$match")
        }.getDocument("\$match")
    }

    private fun processBasicLookup(document: BsonDocument, aggregation: Aggregation, indexes: MutableList<Index>) {
        // Unpack the fields
        val from = document.getString("from").value
        val foreignField = document.getString("foreignField").value
        val aggregationCommand = aggregation.command()

        // Is the collection self-referential
        if (aggregationCommand.collection == from) {
            indexes += SingleFieldIndex("${foreignField}_1", Field(foreignField, IndexDirection.UNKNOWN))
            return
        }

        // We need to lookup the other collection and add the index on it
        // Get the collection
        val collection = collection!!.db.getCollection(Namespace(aggregation.namespace().db, from))

        // Add the operation to the right collection
        collection.addIndex(SingleFieldIndex("${foreignField}_1", Field(foreignField, IndexDirection.UNKNOWN)))
    }

    private fun processQuery(query: Query) {
        val indexes = processQueryCommand(query.command())

        indexes.forEach {
            if (!candidateIndexes.contains(it)) {
                candidateIndexes += it
            }
        }
    }

    private fun processQueryCommand(queryCommand: QueryCommand) : List<Index> {
        val indexes = mutableListOf<Index>()

        // Check if we have a $geoIntersects or $geoWithin query
        if (isGeoQueryIndex(queryCommand)) {
            addGeoQueryIndex(queryCommand, indexes)
            return indexes
        }

        // Do we have query that contains a regular expression
        if (containsRegularExpression(queryCommand)) {
            processRegularExpression(queryCommand, indexes)
            return indexes
        }

        // Check if w have a multikey index
        if (isMultiKeyIndex(queryCommand)) {
            addMultiKeyIndex(queryCommand, indexes)
            return indexes
        }

        // Check if it's a single field index
        if (queryCommand.filter.entries.size == 1) {
            addSingleFieldIndex(queryCommand, indexes)
        }

        // Check if it's a compound index
        if (queryCommand.filter.entries.size > 1) {
            addCompoundFieldIndex(queryCommand, indexes)
        }

        return indexes
    }

    /**
     * We will specify a possible text index for each regular expression entry
     * The left over fields will then be processed against other possible index candidates
     */
    private fun processRegularExpression(query: QueryCommand, candidateIndexes: MutableList<Index>) {
        val regularExpressions = mutableListOf<List<String>>()
        val filteredOutDoc = query.filter.clone()
        val filteredSortDoc = query.sort.clone()

        traverse(query.filter) { _, _path, entry ->
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
            val indexes = processQueryCommand(QueryCommand(query.db, query.collection, filteredOutDoc, filteredSortDoc))

            indexes.forEach {
                if (!candidateIndexes.contains(it)) {
                    candidateIndexes += it
                }
            }
        }
    }

    private fun containsRegularExpression(query: QueryCommand): Boolean {
        var contains = false

        traverse(query.filter) { _, _, entry ->
            if (entry.value is BsonRegularExpression) contains = true
        }

        return contains
    }

    private fun addGeoQueryIndex(query: QueryCommand, candidateIndexes: MutableList<Index>) {
        var path: MutableList<String> = mutableListOf()

        traverse(query.filter) { _, _path, entry ->
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

        traverse(query.filter) { _, _, entry ->
            if (entry.key in listOf("\$geoWithin", "\$near", "\$geoIntersects", "\$nearSphere")) {
                contains = true
            }
        }

        return contains
    }

    fun process(index: Index) {
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

    fun addIndex(index: Index) {
        if (!candidateIndexes.contains(index)) {
            candidateIndexes += index
        }
    }
}