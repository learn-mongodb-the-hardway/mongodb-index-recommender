package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.indexes.Field
import com.mconsulting.indexrecommender.indexes.IdIndex
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.indexes.SingleFieldIndex
import com.mconsulting.indexrecommender.log.LogEntry
import com.mconsulting.indexrecommender.log.LogParser
import com.mconsulting.indexrecommender.profiling.Aggregation
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.io.BufferedReader
import kotlin.test.assertEquals

class AggregationRecommendationTest {
    val usersNamespace = Namespace.parse("mindex_recommendation_tests.users")
    val gamesNamespace = Namespace.parse("mindex_recommendation_tests.games")

    fun simpleLookupAssertions(usersResults: CollectionIndexResults, gamesResults: CollectionIndexResults) {
        // Validate the indexes
        assertEquals(1, usersResults.indexes.size)
        assertEquals(IdIndex(
            "_id_"
        ), usersResults.indexes[0])

        assertEquals(2, gamesResults.indexes.size)
        assertEquals(IdIndex(
            "_id_"
        ), gamesResults.indexes[0])
        assertEquals(SingleFieldIndex(
            "user_id_1",
            Field("user_id", IndexDirection.UNKNOWN)
        ), gamesResults.indexes[1])
    }

    /**
     * Simple lookup query that should trigger two index recommendations
     */
    @Test
    fun simpleLookup() {
        val (usersCollection, gamesCollection) = executeOperation("operations/aggregations/aggregation_simple_lookup.json")
        // Get the results
        simpleLookupAssertions(usersCollection.done(), gamesCollection.done())
    }

    /**
     * Simple lookup query that should trigger two index recommendations from Log
     */
    @Test
    fun simpleLookupFromLog() {
        val (usersCollection, gamesCollection) = executeLogEntry("operations/aggregations/aggregation_simple_lookup.txt")
        // Get the results
        simpleLookupAssertions(usersCollection.done(), gamesCollection.done())
    }

    fun simpleLookupMultipleJoinsAssertions(usersResults: CollectionIndexResults, gamesResults: CollectionIndexResults) {
        // Validate the indexes
        assertEquals(1, usersResults.indexes.size)
        assertEquals(IdIndex(
            "_id_"
        ), usersResults.indexes[0])

        assertEquals(2, gamesResults.indexes.size)
        assertEquals(IdIndex(
            "_id_"
        ), gamesResults.indexes[0])
        assertEquals(SingleFieldIndex(
            "b_1",
            Field("b", IndexDirection.UNKNOWN)
        ), gamesResults.indexes[1])
    }

    /**
     * Simple lookup query multiple joins
     */
    @Test
    fun simpleLookupMultipleJoins() {
        val (usersCollection, gamesCollection) = executeOperation("operations/aggregations/aggregation_multiple_join_conditions.json")
        // Get the results
        simpleLookupMultipleJoinsAssertions(usersCollection.done(), gamesCollection.done())
    }

    /**
     * Simple lookup query multiple joins
     */
    @Test
    fun simpleLookupMultipleJoinsLog() {
        val (usersCollection, gamesCollection) = executeLogEntry("operations/aggregations/aggregation_multiple_join_conditions.txt")
        // Get the results
        simpleLookupMultipleJoinsAssertions(usersCollection.done(), gamesCollection.done())
    }

    fun simpleLookupUncorrelatedJoinsAssertions(usersResults: CollectionIndexResults, gamesResults: CollectionIndexResults) {
        // Validate the indexes
        assertEquals(1, usersResults.indexes.size)
        assertEquals(IdIndex(
            "_id_"
        ), usersResults.indexes[0])

        assertEquals(2, gamesResults.indexes.size)
        assertEquals(IdIndex(
            "_id_"
        ), gamesResults.indexes[0])
        assertEquals(SingleFieldIndex(
            "b_1",
            Field("b", IndexDirection.UNKNOWN)
        ), gamesResults.indexes[1])
    }

    /**
     * Simple lookup query multiple joins
     */
    @Test
    fun simpleLookupUncorrelatedJoins() {
        val (usersCollection, gamesCollection) = executeOperation("operations/aggregations/aggregation_uncorrelated_join.json")
        // Get the results
        simpleLookupUncorrelatedJoinsAssertions(usersCollection.done(), gamesCollection.done())
    }

    @Test
    fun simpleLookupUncorrelatedJoinsLog() {
        val (usersCollection, gamesCollection) = executeLogEntry("operations/aggregations/aggregation_uncorrelated_join.txt")
        // Get the results
        simpleLookupUncorrelatedJoinsAssertions(usersCollection.done(), gamesCollection.done())
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

    private fun executeLogEntry(resource: String): Pair<Collection, Collection> {
        val logParser = LogParser(BufferedReader(readResourceAsReader(resource)))
        val entries = mutableListOf<LogEntry>()
        logParser.forEach {
            entries += it
        }
        val db = Db(client, usersNamespace)
        // Users collection
        val usersCollection = db.getCollection(usersNamespace)
        val gamesCollection = db.getCollection(gamesNamespace)

        // Add the operation
        usersCollection.addLogEntry(entries[0])
        return Pair(usersCollection, gamesCollection)
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
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }
}