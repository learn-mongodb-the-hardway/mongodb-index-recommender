package com.mconsulting.indexrecommender.integration

import com.mconsulting.indexrecommender.Collection
import com.mconsulting.indexrecommender.CollectionOptions
import com.mconsulting.indexrecommender.Namespace
import com.mconsulting.indexrecommender.indexes.Field
import com.mconsulting.indexrecommender.indexes.IdIndex
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.indexes.SingleFieldIndex
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.IndexOptions
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

// Plan
// 1. Create a collection
// 2. Ensure profiling is on
// 3. Perform operation(s)
// 4. Turn off profiling
// 5. Run Recommendation Engine
class SimpleIndexRecommendationTest {
    private var index = 0
    private lateinit var collectionName: String
    private lateinit var collection: MongoCollection<Document>

    private fun createCollectionName() : String {
        return "SimpleIndexRecommendationTest_${index++}"
    }

    private fun turnOffProfiling() {
        val result = db.runCommand(Document(mapOf(
            "profile" to 0
        )))

        assertEquals("1.0", result.get("ok").toString())
    }

    private fun turnOnProfiling(level: Int = 2, slowMs: Int = 0) {
        val result = db.runCommand(Document(mapOf(
            "profile" to level,
            "slowms" to slowMs
        )))

        assertEquals("1.0", result.get("ok").toString())
    }

    @BeforeEach
    fun beforeEach() {
        collectionName = createCollectionName()
        collection = db.getCollection(collectionName)

        if (db.listCollectionNames().firstOrNull { it == collectionName } != null) {
            collection.drop()
        }
    }

    @AfterEach
    fun afterEach() {
        collection.drop()
    }

    @Test
    fun simpleSingleIndexRecommendation() {
        // Turn on profiling
        turnOnProfiling()

        // Insert a document
        collection.insertOne(Document(mapOf("a" to 1)))

        // Execute a query (expect us to get one)
        collection.find(Document(mapOf(
            "a" to 1
        ))).first()

        // Turn off profiling
        turnOffProfiling()

        // Run the recommendation engine
        val coll = Collection(
            client,
            Namespace(db.name, collectionName),
            CollectionOptions(true, true))
        val results = coll.process()

        // Validate that we have the expected indexes
        assertEquals(2, results.indexes.size)
        assertEquals(
            IdIndex("_id_"),
            results.getIndex("_id_"))
        assertEquals(
            SingleFieldIndex("a_1", Field("a", IndexDirection.UNKNOWN)),
            results.getIndex("a_1"))
    }

    companion object {
        lateinit var client: MongoClient
        lateinit var db: MongoDatabase

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            client = MongoClient(MongoClientURI("mongodb://localhost:27017"))
            db = client.getDatabase("mindex_recommendation_tests")
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }
}