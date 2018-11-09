package com.mconsulting.indexrecommender.integration

import com.mconsulting.indexrecommender.indexes.CompoundIndex
import com.mconsulting.indexrecommender.indexes.Field
import com.mconsulting.indexrecommender.indexes.IdIndex
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.indexes.MultikeyIndex
import com.mconsulting.indexrecommender.indexes.SingleFieldIndex
import org.bson.Document
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

// Plan
// 1. Create a collection
// 2. Ensure profiling is on
// 3. Perform operation(s)
// 4. Turn off profiling
// 5. Run Recommendation Engine
class SimpleIndexRecommendationTest : IntegrationTestBase() {
    @Test
    fun simpleSingleIndexRecommendation() {
        // Setup the case
        executeOperations {
            // Insert a document
            collection.insertOne(Document(mapOf("a" to 1)))

            // Execute a query (expect us to get one)
            collection.find(Document(mapOf(
                "a" to 1
            ))).first()
        }

        // Run the recommendation engine
        val results = runRecommendationEngine()

        // Validate that we have the expected indexes
        assertEquals(2, results.indexes.size)
        assertEquals(
            IdIndex("_id_"),
            results.getIndex("_id_"))
        assertEquals(
            SingleFieldIndex("a_1", Field("a", IndexDirection.UNKNOWN)),
            results.getIndex("a_1"))
    }

    @Test
    fun simpleCompoundIndexRecommendation() {
        // Setup the case
        executeOperations {
            // Insert a document
            collection.insertOne(Document(mapOf("a" to 1, "b" to 1)))

            // Execute a query (expect us to get one)
            collection.find(Document(mapOf(
                "a" to 1, "b" to 1
            ))).first()
        }

        // Run the recommendation engine
        val results = runRecommendationEngine()

        // Validate that we have the expected indexes
        assertEquals(2, results.indexes.size)
        assertEquals(
            IdIndex("_id_"),
            results.getIndex("_id_"))
        assertEquals(
            CompoundIndex("a_1_b_1", listOf(
                Field("a", IndexDirection.UNKNOWN),
                Field("b", IndexDirection.UNKNOWN)
            )),
            results.getIndex("a_1_b_1"))
    }

    @Test
    fun simpleMultikeyIndexRecommendation() {
        // Setup the case
        executeOperations {
            // Insert a document
            collection.insertOne(Document(mapOf(
                "a" to 1,
                "b" to listOf(
                    Document(mapOf("c" to 1))
                ))
            ))

            // Execute a query (expect us to get one)
            collection.find(Document(mapOf(
                "a" to 1, "b.c" to 1
            ))).first()
        }

        // Run the recommendation engine
        val results = runRecommendationEngine()

        // Validate that we have the expected indexes
        assertEquals(2, results.indexes.size)
        assertEquals(
            IdIndex("_id_"),
            results.getIndex("_id_"))
        assertEquals(
            MultikeyIndex("a_1_b.c_1", listOf(
                Field("a", IndexDirection.UNKNOWN),
                Field("b.c", IndexDirection.UNKNOWN)
            )),
            results.getIndex("a_1_b.c_1"))
    }
}