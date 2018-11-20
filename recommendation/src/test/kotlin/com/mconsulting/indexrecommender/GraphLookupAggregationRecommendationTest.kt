package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.indexes.Field
import com.mconsulting.indexrecommender.indexes.IdIndex
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.indexes.SingleFieldIndex
import com.mconsulting.indexrecommender.profiling.Aggregation
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class GraphLookupAggregationRecommendationTest {
    val usersNamespace = Namespace.parse("mindex_recommendation_tests.users")
    val gamesNamespace = Namespace.parse("mindex_recommendation_tests.games")

    fun basicGraphLookupAssertions(usersResults: CollectionIndexResults, gamesResults: CollectionIndexResults) {
        // Validate the indexes
        assertEquals(1, usersResults.indexes.size)
        assertEquals(SingleFieldIndex(
            "name_1",
            Field("name", IndexDirection.UNKNOWN)
        ), usersResults.indexes[0])

        assertEquals(0, gamesResults.indexes.size)
    }

    @Test
    fun basicGraphLookup() {
        val (usersCollection, gamesCollection) = executeOperation("operations/aggregations/graphlookup_basic.json")
        basicGraphLookupAssertions(usersCollection.done(), gamesCollection.done())
    }

    fun basicGraphLookupWithMatchAssertions(usersResults: CollectionIndexResults, gamesResults: CollectionIndexResults) {
        // Validate the indexes
        assertEquals(2, usersResults.indexes.size)
        assertEquals(SingleFieldIndex(
            "name_1",
            Field("name", IndexDirection.UNKNOWN)
        ), usersResults.indexes[0])
        assertEquals(SingleFieldIndex(
            "user_id_1",
            Field("user_id", IndexDirection.UNKNOWN)
        ), usersResults.indexes[1])

        assertEquals(0, gamesResults.indexes.size)
    }

    @Test
    fun basicGraphLookupWithMatch() {
        val (usersCollection, gamesCollection) = executeOperation("operations/aggregations/graphlookup_basic_with_match.json")
        basicGraphLookupWithMatchAssertions(usersCollection.done(), gamesCollection.done())
    }

    private fun executeOperation(resource: String): Pair<Collection, Collection> {
        val operation = Aggregation(readJsonAsBsonDocument(resource))
        val db = Db(client, usersNamespace)

        // Users collection
        val usersCollection = db.getCollection(usersNamespace)
        val gamesCollection = db.getCollection(gamesNamespace)

        // Add the operation
        usersCollection.addOperation(operation)
        return Pair(usersCollection, gamesCollection)
    }

    companion object {
        lateinit var client: MongoClient
        lateinit var db: MongoDatabase

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            client = MongoClient(MongoClientURI("mongodb://localhost:27017"))
            db = client.getDatabase("mindex_recommendation_tests")

            db.getCollection("users").drop()
            db.getCollection("games").drop()
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }
}