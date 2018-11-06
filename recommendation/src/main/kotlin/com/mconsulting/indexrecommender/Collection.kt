package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.indexes.Index
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.indexes.IndexParser
import com.mconsulting.indexrecommender.indexes.IndexParserOptions
import com.mconsulting.indexrecommender.profiling.Aggregation
import com.mconsulting.indexrecommender.profiling.Operation
import com.mconsulting.indexrecommender.profiling.Query
import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.BsonArray
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonElement
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.BsonValue

data class CollectionOptions(
    val allowExplainExecution: Boolean = true,
    val executeQueries: Boolean = true
)

class Collection(
    val client: MongoClient,
    namespace: Namespace,
    options: CollectionOptions = CollectionOptions()) {

    private var database: MongoDatabase
    private var collection: MongoCollection<BsonDocument>
    private var systemProfileCollection: MongoCollection<BsonDocument>
    private var documentCount: Long = -1
    private var existingIndexes: List<Index> = listOf()
    private var statisticsProcessor: StatisticsProcessor = StatisticsProcessor()

    // Index parser
    private val indexParser = IndexParser(client, IndexParserOptions(
        allowExplainExecution = options.allowExplainExecution
    ))

    // Recommendation engine
    private var recommendationEngine: IndexRecommendationEngine = IndexRecommendationEngine(client, IndexRecommendationOptions(
        executeQueries = options.executeQueries
    ))

    init {
        database = client.getDatabase(namespace.db)
        collection = database.getCollection(namespace.collection, BsonDocument::class.java)
        systemProfileCollection = database.getCollection("system.profile", BsonDocument::class.java)
    }

    fun count() : Long {
        if (documentCount > 0) return documentCount
        documentCount = collection.count()
        return documentCount
    }

    fun readExistingIndexes() : List<Index> {
        if (existingIndexes.isEmpty()) {
            existingIndexes = collection.listIndexes(BsonDocument::class.java).map {
                indexParser.createIndex(it)
            }.toList()
        }

        return existingIndexes
    }

    fun processProfilingInformation() {
        collection.find().sort(BsonDocument()
            .append("ts", BsonInt32(IndexDirection.ASCENDING.value()))).map {
            createOperation(it)
        }.forEach {
            if (it != null) {
                // Add the shape to the recommendation engine
                recommendationEngine.add(it)
                // Collect information about the query shape
                statisticsProcessor.process(it)
            }
        }
    }
}

private fun createOperation(doc: BsonDocument): Operation? {
    var operation: Operation? = null

    if (doc.getString("op") == BsonString("query")) {
        return Query(doc)
    } else if (doc.getString("op") == BsonString("command")) {
        // Identify the operation
        // (we care only about operations that contain reads (query, agg, update, count etc.)
        val command = doc.getDocument("command")

        // We have an agregation command
        if (command.containsKey("aggregate")) {
            return Aggregation(doc)
        } else if (command.containsKey("count")) {
            throw NotImplementedError("count command not implemented")
        } else if (command.containsKey("update")) {
            throw NotImplementedError("update command not implemented")
        }
    }

    return operation
}
