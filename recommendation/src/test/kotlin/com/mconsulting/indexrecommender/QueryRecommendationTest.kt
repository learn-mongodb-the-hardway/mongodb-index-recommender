package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.indexes.CompoundIndex
import com.mconsulting.indexrecommender.indexes.Field
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.indexes.SingleFieldIndex
import com.mconsulting.indexrecommender.profiling.Query
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class QueryRecommendationTest {

    /**
     * Single field queries
     */
    @Test
    fun singleField() {
        val operation = Query(readJsonAsBsonDocument("operations/top_level_single_field_query.json"))

        // Create index recommendation engine and add operation
        val recommender = IndexRecommendationEngine(client)
        recommender.add(operation)

        // Return the recommendation
        val recommendation = recommender.recommend()

        // Validate the indexes
        assertEquals(1, recommendation.indexes.size)
        assertEquals(recommendation.indexes[0], SingleFieldIndex(
            "name_-1",
            Field("name", IndexDirection.UNKNOWN)
        ))
    }

    @Test
    fun singleFieldAndSort() {
        val operation = Query(readJsonAsBsonDocument("operations/top_level_single_field_query_with_sort.json"))

        // Create index recommendation engine and add operation
        val recommender = IndexRecommendationEngine(client)
        recommender.add(operation)

        // Return the recommendation
        val recommendation = recommender.recommend()

        // Validate the indexes
        assertEquals(1, recommendation.indexes.size)
        assertEquals(recommendation.indexes[0], SingleFieldIndex(
            "name_-1",
            Field("name", IndexDirection.DESCENDING)
        ))
    }

    /**
     * Two field queries
     */
    @Test
    fun twoTopLevelFields() {
        val operation = Query(readJsonAsBsonDocument("operations/top_level_two_field_query.json"))

        // Create index recommendation engine and add operation
        val recommender = IndexRecommendationEngine(client)
        recommender.add(operation)

        // Return the recommendation
        val recommendation = recommender.recommend()

        // Validate the indexes
        assertEquals(1, recommendation.indexes.size)
        assertEquals(recommendation.indexes[0], CompoundIndex(
            "name_-1_number_-1",
            listOf(
                Field("name", IndexDirection.UNKNOWN),
                Field("number", IndexDirection.UNKNOWN)
            )
        ))
    }

    @Test
    fun twoLevelFieldsAndSort() {
        val operation = Query(readJsonAsBsonDocument("operations/top_level_two_field_query_with_sort.json"))

        // Create index recommendation engine and add operation
        val recommender = IndexRecommendationEngine(client)
        recommender.add(operation)

        // Return the recommendation
        val recommendation = recommender.recommend()

        // Validate the indexes
        assertEquals(1, recommendation.indexes.size)
        assertEquals(recommendation.indexes[0], CompoundIndex(
            "name_-1_number_-1",
            listOf(
                Field("name", IndexDirection.DESCENDING),
                Field("number", IndexDirection.ASCENDING)
            )
        ))
    }

    /**
     * Multikey query
     */
    @Test
    fun multiKeyFieldQuery() {
        val operation = Query(readJsonAsBsonDocument("operations/multi_key_query.json"))

        // Create index recommendation engine and add operation
        val recommender = IndexRecommendationEngine(client)
        recommender.add(operation)

        // Return the recommendation
        val recommendation = recommender.recommend()
        println()
    }

    companion object {
        lateinit var client: MongoClient
        lateinit var db: MongoDatabase
        lateinit var collection: MongoCollection<Document>

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            client = MongoClient(MongoClientURI("mongodb://localhost:27017"))
            db = client.getDatabase("mindex_recommendation_tests")
            collection = db.getCollection("index_tests")

            // Drop collection
            collection.drop()

            // Insert a test document
            collection.insertOne(Document(mapOf(
                "a" to listOf(Document(mapOf(
                    "b" to 1
                )))
            )))

            // Generate test indexes
            collection.createIndex(Document(mapOf(
                "a.b" to 1
            )))
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }
}