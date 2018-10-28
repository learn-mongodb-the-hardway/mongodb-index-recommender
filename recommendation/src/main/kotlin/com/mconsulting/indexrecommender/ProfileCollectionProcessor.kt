package com.mconsulting.indexrecommender.profiling

import com.mconsulting.indexrecommender.IndexRecommendationEngine
import com.mconsulting.indexrecommender.IndexRecommendations
import com.mongodb.MongoClient
import org.bson.BsonDocument
import org.bson.BsonString
import org.bson.Document

class ProfileCollectionProcessorOptions()

class ProfileCollectionProcessor(
    val client: MongoClient,
    val db: String,
    val collection: String,
    val options: ProfileCollectionProcessorOptions = ProfileCollectionProcessorOptions()) {
    val indexRecommendationEngine = IndexRecommendationEngine()

    /**
     * Process all the entries in the profile
     */
    fun process() : IndexRecommendations {
        // Get the system profile collection
        val collection = client
            .getDatabase("admin")
            .getCollection("system.profile", BsonDocument::class.java)

        val cursor = collection.find(Document(mapOf(
            "ns" to "$db.$collection"
        )))

        // Get the documents
        for (doc in cursor) {
            val operation = createOperation(doc)
            if (operation != null) {
                indexRecommendationEngine.add(operation)
            }
        }

        // Return the recommendation
        return indexRecommendationEngine.recommend()
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
}