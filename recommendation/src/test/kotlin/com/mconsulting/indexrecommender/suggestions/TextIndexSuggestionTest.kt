package com.mconsulting.indexrecommender.suggestions

import com.mconsulting.indexrecommender.indexes.Field
import com.mconsulting.indexrecommender.indexes.IdIndex
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.indexes.SingleFieldIndex
import com.mconsulting.indexrecommender.indexes.TextField
import com.mconsulting.indexrecommender.indexes.TextIndex
import com.mconsulting.indexrecommender.integration.IntegrationTestBase
import org.bson.BsonRegularExpression
import org.bson.Document
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test

class TextIndexSuggestionTest : IntegrationTestBase() {

    @Test
    fun shouldSuggestTextIndexDueToRegExpFound() {
        // Setup the case
        executeOperations {
            // Insert a document
            collection.insertOne(Document(mapOf(
                "b" to mapOf(
                    "text" to "hello world what is going on"
                ),
                "a" to mapOf(
                    "b" to 1
                )
            )))

            //{ location: { $geoWithin: { $center: [ [ -73.856077, 40.848447 ], 10 ] } } }
            // Execute a query (expect us to get one)
            val doc = collection.find(Document(mapOf(
                "b.text" to BsonRegularExpression("world", "i"),
                "a" to mapOf(
                    "b" to 1
                )
            ))).firstOrNull()
            assertNotNull(doc)
        }

        // Run the recommendation engine
        val results = runRecommendationEngine()

        // Validate that we have the expected indexes
        assertEquals(3, results.indexes.size)
        assertEquals(
            IdIndex("_id_"),
            results.getIndex("_id_"))
        assertEquals(
            TextIndex("b.text_1", listOf(TextField(listOf("b.text"), 1))),
            results.getIndex("b.text_1"))
        assertEquals(
            SingleFieldIndex("a_1", Field("a", IndexDirection.UNKNOWN)),
            results.getIndex("a_1"))
    }
}