package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.indexes.Index
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.indexes.IndexParser
import com.mconsulting.indexrecommender.indexes.IndexParserOptions
import com.mconsulting.indexrecommender.profiling.Aggregation
import com.mconsulting.indexrecommender.profiling.Delete
import com.mconsulting.indexrecommender.profiling.Insert
import com.mconsulting.indexrecommender.profiling.Operation
import com.mconsulting.indexrecommender.profiling.Query
import com.mconsulting.indexrecommender.profiling.Update
import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString

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
}

class Collection(
    val client: MongoClient,
    val namespace: Namespace,
    options: CollectionOptions = CollectionOptions()) {

    private var database: MongoDatabase
    private var collection: MongoCollection<BsonDocument>
    private var systemProfileCollection: MongoCollection<BsonDocument>
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

    fun process(processProfile: Boolean = true) : CollectionIndexResults {
        // Read the existing indexes
        existingIndexes = collection.listIndexes(BsonDocument::class.java).map {
            indexParser.createIndex(it)
        }.toList()

        // Feed the existing indexes to the recommendation engine
        existingIndexes.forEach {
            recommendationEngine.add(it)
        }

        // Process the profiling information
        if (processProfile) {
            processProfilingInformation()
        }

        // Return the results
        val recommendedIndexes = recommendationEngine.recommend()
        val shapeStatistics = statisticsProcessor.done()

        // Return the
        return CollectionIndexResults(
            namespace = namespace,
            indexes = recommendedIndexes,
            shapeStatistics = shapeStatistics
        )
    }

    private fun processProfilingInformation() {
        systemProfileCollection.find().sort(BsonDocument()
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

fun createOperation(doc: BsonDocument): Operation? {
    var operation: Operation? = null

    if (doc.getString("op") == BsonString("query")) {
        return Query(doc)
    } else if (doc.getString("op") == BsonString("insert")) {
        return Insert(doc)
    } else if (doc.getString("op") == BsonString("remove")) {
        return Delete(doc)
    } else if (doc.getString("op") == BsonString("update")) {
        return Update(doc)
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
