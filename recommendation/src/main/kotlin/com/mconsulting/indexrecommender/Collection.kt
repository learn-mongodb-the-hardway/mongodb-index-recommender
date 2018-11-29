package com.mconsulting.indexrecommender

import com.beust.klaxon.JsonObject
import com.mconsulting.indexrecommender.indexes.Index
import com.mconsulting.indexrecommender.indexes.IndexParser
import com.mconsulting.indexrecommender.indexes.IndexParserOptions
import com.mconsulting.indexrecommender.log.LogEntry
import com.mconsulting.indexrecommender.profiling.Aggregation
import com.mconsulting.indexrecommender.profiling.Count
import com.mconsulting.indexrecommender.profiling.Delete
import com.mconsulting.indexrecommender.profiling.GeoNear
import com.mconsulting.indexrecommender.profiling.Insert
import com.mconsulting.indexrecommender.profiling.NotSupportedOperation
import com.mconsulting.indexrecommender.profiling.Operation
import com.mconsulting.indexrecommender.profiling.Query
import com.mconsulting.indexrecommender.profiling.Update
import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import mu.KLogging
import mu.KotlinLogging
import org.bson.BsonDocument

data class CollectionOptions(
    val allowExplainExecution: Boolean = true,
    val executeQueries: Boolean = true
)

data class CollectionIndexResults(
    val namespace: Namespace,
    val indexes: List<Index>,
    val shapeStatistics: List<ShapeStatistics>
) {
    fun getIndex(name: String): Index? {
        return indexes.firstOrNull { it.name == name }
    }

    fun contains(name: String): Boolean {
        return getIndex(name) != null
    }
}

class Collection(
    val client: MongoClient,
    val namespace: Namespace,
    val db: Db,
    options: CollectionOptions = CollectionOptions()) {

    private var database: MongoDatabase = client.getDatabase(namespace.db)
    private var collection: MongoCollection<BsonDocument>
    private var existingIndexes: List<Index> = listOf()
    private var statisticsProcessor: StatisticsProcessor = StatisticsProcessor()

    // Index parser
    private val indexParser = IndexParser(client, IndexParserOptions(
        allowExplainExecution = options.allowExplainExecution
    ))

    // Recommendation engine
    private var recommendationEngine: IndexRecommendationEngine = IndexRecommendationEngine(client, this)

    init {
        collection = database.getCollection(namespace.collection, BsonDocument::class.java)
        // Process any existing indexes
        processExistingIndexes()
    }

    private fun processExistingIndexes() {
        // Read the existing indexes
        existingIndexes = collection.listIndexes(BsonDocument::class.java).map {
            indexParser.createIndex(it)
        }.toList()

        // Feed the existing indexes to the recommendation engine
        existingIndexes.forEach {
            recommendationEngine.process(it)
        }
    }

    fun done() : CollectionIndexResults {
        return CollectionIndexResults(
            namespace = namespace,
            indexes = recommendationEngine.recommend(),
            shapeStatistics = statisticsProcessor.done()
        )
    }

    fun addLogEntry(logEntry: LogEntry) {
        recommendationEngine.process(logEntry)
        statisticsProcessor.process(logEntry)
    }

    fun addOperation(operation: Operation) {
        recommendationEngine.process(operation)
        statisticsProcessor.process(operation)
    }

    fun addIndex(index: Index) {
        recommendationEngine.addIndex(index)
    }

    companion object : KLogging()
}

private val logger = KotlinLogging.logger { }

fun createOperation(doc: JsonObject): Operation? {
    val operation = doc.string("op")

    return when (operation) {
        "query" -> Query(doc)
        "insert" -> Insert(doc)
        "remove" -> Delete(doc)
        "update" -> Update(doc)
        "command" -> {
            // Identify the operation
            // (we care only about operations that contain reads (query, agg, update, count etc.)
            val command = doc.obj("command")!!

            return when {
                command.containsKey("aggregate") -> Aggregation(doc)
                command.containsKey("geoNear") -> GeoNear(doc)
                command.containsKey("count") -> Count(doc)
                else -> {
                    logger.warn { "Failed to create operation from [${doc.toJsonString()}" }
                    NotSupportedOperation(doc)
                }
            }
        }
        else -> {
            logger.warn { "Failed to create operation from [${doc.toJsonString()}" }
            NotSupportedOperation(doc)
        }
    }
}