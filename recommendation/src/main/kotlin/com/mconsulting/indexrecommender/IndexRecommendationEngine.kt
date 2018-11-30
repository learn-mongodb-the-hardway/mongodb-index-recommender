package com.mconsulting.indexrecommender

import com.beust.klaxon.JsonArray
import com.beust.klaxon.JsonObject
import com.beust.klaxon.JsonValue
import com.mconsulting.indexrecommender.indexes.CompoundIndex
import com.mconsulting.indexrecommender.indexes.Field
import com.mconsulting.indexrecommender.indexes.Index
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.indexes.MultikeyIndex
import com.mconsulting.indexrecommender.indexes.SingleFieldIndex
import com.mconsulting.indexrecommender.indexes.TextField
import com.mconsulting.indexrecommender.indexes.TextIndex
import com.mconsulting.indexrecommender.indexes.TwoDSphereIndex
import com.mconsulting.indexrecommender.log.CommandLogEntry
import com.mconsulting.indexrecommender.log.LogEntry
import com.mconsulting.indexrecommender.profiling.Aggregation
import com.mconsulting.indexrecommender.profiling.AggregationCommand
import com.mconsulting.indexrecommender.profiling.NotSupportedOperation
import com.mconsulting.indexrecommender.profiling.Operation
import com.mconsulting.indexrecommender.profiling.Query
import com.mconsulting.indexrecommender.profiling.QueryCommand
import com.mconsulting.indexrecommender.profiling.Update
import com.mongodb.MongoClient
import mu.KLogging
import org.bson.BsonDocument

class IndexRecommendationOptions()

fun JsonObject.remove(path: List<String>) {
    var pointer: Any? = this
    var previousPointer: Any? = this

    for(entry in path) {
        previousPointer = pointer
        pointer = when (pointer) {
            is JsonObject -> pointer[entry]
            is JsonArray<*> -> pointer[entry.toInt()]
            else -> pointer
        }
    }

    when (previousPointer) {
        is JsonObject -> previousPointer.remove(path.last())
        is JsonArray<*> -> previousPointer.removeAt(path.last().toInt())
    }
}

class IndexRecommendationEngine(
    val client: MongoClient,
    val collection: Collection? = null,
    val options: IndexRecommendationOptions = IndexRecommendationOptions()) {

    private val candidateIndexes = mutableListOf<Index>()

    fun process(operation: Operation) {
        if (logger.isDebugEnabled) logger.debug { "Processing operation [${operation.doc.toJsonString()}" }

        when (operation) {
            is Query -> processQuery(operation)
            is Update -> processUpdate(operation)
            is Aggregation -> processAggregation(operation)
            is NotSupportedOperation -> logger.warn { "Attempting to process a non supported operation" }
        }
    }

    fun process(logEntry: LogEntry) {
        when (logEntry) {
            is CommandLogEntry -> processCommandLogEntry(logEntry)
        }
    }

    private fun processCommandLogEntry(logEntry: CommandLogEntry) {
        when (logEntry.commandName) {
            "aggregate" -> {
                processAggregationCommand(AggregationCommand(
                    logEntry.namespace.db,
                    logEntry.namespace.collection,
                    logEntry.command.array<JsonObject>("pipeline")!!))
            }
        }
    }

    private fun processAggregation(aggregation: Aggregation) {
        processAggregationCommand(aggregation.command()).forEach {
            if (!candidateIndexes.contains(it)) {
                candidateIndexes += it
            }
        }
    }

    private fun processAggregationCommand(aggregationCommand: AggregationCommand) : List<Index> {
        val indexes = mutableListOf<Index>()
        // First match we have seen
        var firstMatchSeen = false
        var firstLookupSeen = false
        var firstGraphLookup = false

        // Identify any lookup fields
        aggregationCommand
            .pipeline
            .filterIsInstance<JsonObject>()
            .forEachIndexed { _, jsonObject ->
                // Do we have a $match stage (only look at the first one that shows)
                if (jsonObject.containsKey("\$match")
                    && !firstLookupSeen && !firstMatchSeen && !firstGraphLookup) {
                    addMatchIndex(indexes, aggregationCommand, jsonObject)
                    firstMatchSeen = true
                }

                // Do we have a $lookup stage
                if (jsonObject.containsKey("\$lookup")) {
                    addLookupIndex(aggregationCommand, jsonObject)
                    firstLookupSeen = true
                }

                // Do we have a $graphLookup stage
                if (jsonObject.containsKey("\$graphLookup")) {
                    addGraphLookupIndex(aggregationCommand, jsonObject)
                    firstGraphLookup = true
                }
            }

        // For each index add it to the candidate indexes
        return indexes
    }

    private fun addMatchIndex(indexes: MutableList<Index>, aggregationCommand: AggregationCommand, bsonDocument: JsonObject) {
        val matchDocument = bsonDocument.obj("\$match")!!
        // Process the match as a query command
        val candidateIndexes = processQueryCommand(QueryCommand(aggregationCommand.db, aggregationCommand.collection, matchDocument, JsonObject()))
        /// Add any missed indexes
        candidateIndexes.forEach {
            if (!indexes.contains(it)) {
                indexes += it
            }
        }
    }

    private fun addGraphLookupIndex(aggregationCommand: AggregationCommand, document: JsonObject) {
        // Get the $graphLookup
        val graphLookupDocument = document.obj("\$graphLookup")!!
        val from = graphLookupDocument.string("from")!!
        val connectToField = graphLookupDocument.string("connectToField")!!

        // Get the referenced collection
        val collection = collection!!.db.getCollection(Namespace(aggregationCommand.db, from))
        val candidateIndexes = mutableListOf<Index>()

        // Add the graphLookup candidate index
        candidateIndexes += SingleFieldIndex("${connectToField}_1", Field(connectToField, IndexDirection.UNKNOWN))

        // Do we have a filter on the graphlookup, this is a possible candidate
        // for an index
        if  (graphLookupDocument.containsKey("restrictSearchWithMatch")) {
            val restrictSearchWithMatch = graphLookupDocument.obj("restrictSearchWithMatch")!!
            // Flatten the matches ( a: { b:1 } becomes a.b
            candidateIndexes.addAll(processQueryCommand(QueryCommand(aggregationCommand.db, aggregationCommand.collection, restrictSearchWithMatch, JsonObject())))
        }

        // Merge in all the possible indexes into the collection
        candidateIndexes.forEach {
            collection.addIndex(it)
        }
    }

    private fun addLookupIndex(aggregation: AggregationCommand, document: JsonObject) {
        val lookupDocument = document.obj("\$lookup")!!

        if (lookupDocument.containsKey("localField")) {
            processBasicLookup(lookupDocument, aggregation)
        } else if (lookupDocument.containsKey("pipeline")) {
            processAdvancedLookup(lookupDocument, aggregation)
        }
    }

    private fun processAdvancedLookup(document: JsonObject, aggregation: AggregationCommand) {
        // Unpack the fields
        val from = document.string("from")!!
        val pipeline = document.array<JsonObject>("pipeline")!!

        // Locate any $match expression
        val matchExpression = extractMatchStatement(pipeline)

        // We need to lookup the other collection and add the index on it
        // Get the collection
        val collection = collection!!.db.getCollection(Namespace(aggregation.db, from))

        // Process the match expression using the same code path as a QUERY
        if (!matchExpression.containsKey("\$expr")) {
            val indexes = processQueryCommand(QueryCommand(aggregation.db, from, matchExpression, JsonObject()))

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

    private fun extractFieldNamesFromMatchExpression(matchExpression: JsonObject): List<String> {
        val fieldNames = mutableListOf<String>()

        traverse(matchExpression) { _, _, entry ->
            // Do we have an $expr for the comparison
            val value = entry.value
            if (value is JsonArray<*>) {
                when (entry.key) {
                    "\$eq",
                    "\$gt",
                    "\$gte",
                    "\$lt",
                    "\$lte",
                    "\$cmp",
                    "\$ne" -> {
                        var fieldName = (entry.value as JsonArray<*>)[0] as String
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

    private fun extractMatchStatement(pipeline: JsonArray<JsonObject>): JsonObject {
        return pipeline.first {
            it.containsKey("\$match")
        }.obj("\$match")!!
    }

    private fun processBasicLookup(document: JsonObject, aggregation: AggregationCommand) {
        // Unpack the fields
        val from = document.string("from")!!
        val foreignField = document.string("foreignField")!!
        val index = SingleFieldIndex("${foreignField}_1", Field(foreignField, IndexDirection.UNKNOWN))

        // We need to lookup the other collection and add the index on it
        // Get the collection
        val collection = collection!!.db.getCollection(Namespace(aggregation.db, from))
        // Add the operation to the right collection
        collection.addIndex(index)
    }

    private fun processUpdate(update: Update) {
        update.command().toQueryCommands().forEach { queryCommand ->
            val indexes = processQueryCommand(queryCommand)

            indexes.forEach { index ->
                if (!candidateIndexes.contains(index)) {
                    candidateIndexes += index
                }
            }
        }
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

        // No filter set up at all, no indexes to discover
        if (queryCommand.filter.isEmpty()) {
            return indexes
        }

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

        // Flatten the index into dot notation for the create index statements
        val flattenedQuery = generateProjection(queryCommand.filter)

        try {
            // Check if w have a multikey index
            if (isMultiKeyIndex(queryCommand)) {
                addMultiKeyIndex(flattenedQuery, queryCommand, indexes)
                return indexes
            }
        } catch (err: Exception) {
            logger.warn { "Failed to sample Operation [${queryCommand.namespace}] - [${queryCommand.filter.toJsonString()}" }
        }

        // Check if it's a single field index
        if (flattenedQuery.size == 1) {
            addSingleFieldIndex(flattenedQuery, queryCommand, indexes)
        }

        // Check if it's a compound index
        if (flattenedQuery.size > 1) {
            addCompoundFieldIndex(flattenedQuery, queryCommand, indexes)
        }

        return indexes
    }

    /**
     * We will specify a possible text index for each regular expression entry
     * The left over fields will then be processed against other possible index candidates
     */
    private fun processRegularExpression(query: QueryCommand, candidateIndexes: MutableList<Index>) {
        val regularExpressions = mutableListOf<List<String>>()
        val filteredOutDoc = JsonObject(query.filter.toMap())
        val filteredSortDoc = JsonObject(query.sort.toMap())
        val queryFilterNames = JsonObject()

        traverse(query.filter) { _, _path, entry ->
            if (entry.value is JsonObject && isBsonRegularExpression(entry.value as JsonObject)) {
                // Add to the list of regular expressions
                regularExpressions.add(_path.toList() + entry.key)
                // Remove from the filtered out Doc
                filteredOutDoc.remove(_path + entry.key)
                // Remove from the sort
                filteredSortDoc.remove(_path + entry.key)
                // Add to the BsonDocument that will be used for naming
                queryFilterNames.put("${(_path + entry.key).joinToString(".")}", 1)
            }
        }

        // Generate a text index recommendation
        candidateIndexes += TextIndex(
            createIndexName(queryFilterNames, QueryCommand(query.db, query.collection, JsonObject(regularExpressions.map {
                it.joinToString(".") to 1
            }.toMap()), JsonObject())), regularExpressions.map {
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
            if (entry.value is JsonObject && isBsonRegularExpression(entry.value as JsonObject)) contains = true
        }

        return contains
    }

    private fun isBsonRegularExpression(value: JsonObject): Boolean {
        if ((value.containsKey("\$regex") && value.size == 1)
            || (value.containsKey("\$regex") && value.containsKey("\$options") && value.size == 2)) {
            return true
        }

        return false
    }

    private fun addGeoQueryIndex(query: QueryCommand, candidateIndexes: MutableList<Index>) {
        var path: MutableList<String> = mutableListOf()

        traverse(query.filter) { _, _path, entry ->
            if (entry.key in listOf("\$geoWithin", "\$near", "\$geoIntersects", "\$nearSphere", "\$within")) {
               path.addAll(_path)
            }
        }

        // If we have a path, we have a geo index candidate
        if (path.isNotEmpty()) {
            candidateIndexes += TwoDSphereIndex(createIndexName(query.filter, query), path.joinToString("."))
        }
   }

    private fun isGeoQueryIndex(query: QueryCommand): Boolean {
        var contains = false

        traverse(query.filter) { _, _, entry ->
            if (entry.key in listOf("\$geoWithin", "\$near", "\$geoIntersects", "\$nearSphere", "\$within")) {
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
            .find(BsonDocument.parse(query.filter.toJsonString()))
            .projection(BsonDocument.parse(generateProjection(query.filter).toJsonString()))
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

    private fun addMultiKeyIndex(query: JsonObject, queryCommand: QueryCommand, candidateIndexes: MutableList<Index>) {
        // Create list of fields
        val fields = query.entries.map {
            Field(it.key, getIndexDirection(queryCommand, it.key, IndexDirection.UNKNOWN))
        }

        // Create index name
        val fieldName = createIndexName(query, queryCommand)

        // Create Multikey index
        val index = MultikeyIndex(fieldName, fields)

        // Add to candidate list if it does not already exist in it
        if (!candidateIndexes.contains(index)) {
            candidateIndexes += index
        }
    }

    private fun addCompoundFieldIndex(query: JsonObject, queryCommand: QueryCommand, candidateIndexes: MutableList<Index>) {
        // Create list of fields
        val fields = query.entries.map {
            Field(it.key, getIndexDirection(queryCommand, it.key, IndexDirection.UNKNOWN))
        }

        // Create index name
        val fieldName = createIndexName(query, queryCommand)

        // Create compound index
        val index = CompoundIndex(fieldName, fields)

        // Add to candidate list if it does not already exist in it
        if (!candidateIndexes.contains(index)) {
            candidateIndexes += index
        }
    }

    private fun addSingleFieldIndex(query: JsonObject, queryCommand: QueryCommand, candidateIndexes: MutableList<Index>) {
        // Get the first entry
        val entry = query.entries.first()
        val fieldName = createIndexName(query, queryCommand)

        // Create the index entry
        val index = SingleFieldIndex(
            fieldName,
            Field(entry.key, getIndexDirection(queryCommand, entry.key, IndexDirection.UNKNOWN)))

        // Check if the key exists
        if (!candidateIndexes.contains(index)) {
            candidateIndexes += index
        }
    }

    private fun getIndexDirection(query: QueryCommand, fieldName: String, defaultDirection: IndexDirection = IndexDirection.ASCENDING): IndexDirection {
        var direction = defaultDirection

        if (query.sort.containsKey(fieldName)) {
            direction = IndexDirection.intValueOf(query.sort.int(fieldName)!!)
        }

        return direction
    }

    private fun createIndexName(query: JsonObject, queryCommand: QueryCommand): String {
        return query.entries.map {
            "${it.key}_${getIndexDirection(queryCommand, it.key).value()}"
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

    companion object : KLogging()
}