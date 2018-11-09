package com.mconsulting.indexrecommender.integration

import com.mconsulting.indexrecommender.Collection
import com.mconsulting.indexrecommender.CollectionIndexResults
import com.mconsulting.indexrecommender.CollectionOptions
import com.mconsulting.indexrecommender.Namespace
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import kotlin.test.assertEquals

abstract class IntegrationTestBase {
    private  var index = 0
    private lateinit var collectionName: String
    protected  lateinit var collection: MongoCollection<Document>

    private  fun createCollectionName() : String {
        return "SimpleIndexRecommendationTest_${index++}"
    }

    private  fun turnOffProfiling() {
        val result = db.runCommand(Document(mapOf(
            "profile" to 0
        )))

        assertEquals("1.0", result["ok"].toString())
    }

    private  fun turnOnProfiling(level: Int = 2, slowMs: Int = 0) {
        // Ensure profiling is off so we can drop the profile collection
        turnOffProfiling()

        // Drop the system profile
        db.getCollection("system.profile").drop()

        // Turn on profiling
        val result = db.runCommand(Document(mapOf(
            "profile" to level,
            "slowms" to slowMs
        )))

        assertEquals("1.0", result["ok"].toString())
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

    protected fun runRecommendationEngine(): CollectionIndexResults {
        // Run the recommendation engine
        val coll = Collection(
            client,
            Namespace(db.name, collectionName),
            CollectionOptions(true, true))
        return coll.process()
    }

    protected fun executeOperations(setup: () -> Unit) {
        // Turn on profiling
        turnOnProfiling()

        // Execute setup
        setup()

        // Turn off profiling
        turnOffProfiling()
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