package com.mconsulting.indexrecommender.suggestions

import com.mconsulting.indexrecommender.Processor
import com.mconsulting.indexrecommender.indexes.Field
import com.mconsulting.indexrecommender.indexes.IdIndex
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.indexes.SingleFieldIndex
import com.mconsulting.indexrecommender.indexes.TextField
import com.mconsulting.indexrecommender.indexes.TextIndex
import com.mconsulting.indexrecommender.ingress.ProfileCollectionIngress
import com.mconsulting.indexrecommender.integration.IntegrationTestBase
import org.bson.BsonRegularExpression
import org.bson.Document
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import kotlin.test.assertTrue

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

        // Set up the processor
        val processor = Processor(client, listOf(this.namespace))
        processor.addSource(ProfileCollectionIngress(client, this.namespace))

        // Process
        val results = processor.process()
        // Get the indexes
        val indexes = results.getIndexes(this.namespace)

        // Validate that we have the expected indexes
        assertEquals(3, indexes.size)
        assertEquals(
            IdIndex("_id_"),
            results.getIndex(this.namespace, "_id_"))
        assertEquals(
            TextIndex("b.text_1", listOf(TextField(listOf("b.text"), 1))),
            results.getIndex(this.namespace, "b.text_1"))
        assertEquals(
            SingleFieldIndex("a_1", Field("a", IndexDirection.UNKNOWN)),
            results.getIndex(this.namespace, "a_1"))
    }

    @Test
    fun shouldCompactTwoRegularExpressionsIntoSingleTextIndexSuggestion() {
        // Setup the case
        executeOperations {
            // Insert a document
            collection.insertOne(Document(mapOf(
                "b" to mapOf(
                    "text" to "hello world what is going on",
                    "text2" to "peter pan"
                ),
                "a" to mapOf(
                    "b" to 1
                )
            )))

            //{ location: { $geoWithin: { $center: [ [ -73.856077, 40.848447 ], 10 ] } } }
            // Execute a query (expect us to get one)
            val doc = collection.find(Document(mapOf(
                "b.text" to BsonRegularExpression("world", "i"),
                "b.text2" to BsonRegularExpression("peter", "i"),
                "a" to mapOf(
                    "b" to 1
                )
            ))).firstOrNull()
            assertNotNull(doc)
        }

        // Set up the processor
        val processor = Processor(client, listOf(this.namespace))
        processor.addSource(ProfileCollectionIngress(client, this.namespace))

        // Process
        val results = processor.process()
        // Get the indexes
        val indexes = results.getIndexes(this.namespace)

        // Validate that we have the expected indexes
        assertEquals(3, indexes.size)
        assertTrue(results.contains(this.namespace, "_id_"))
        assertTrue(results.contains(this.namespace, "b.text_1_b.text2_1"))
        assertTrue(results.contains(this.namespace, "a_1"))
        assertEquals(
            IdIndex("_id_"),
            results.getIndex(this.namespace, "_id_"))
        assertEquals(
            TextIndex("b.text_1_b.text2_1", listOf(
                TextField(listOf("b.text"), 1),
                TextField(listOf("b.text2"), 1)
            )),
            results.getIndex(this.namespace, "b.text_1_b.text2_1"))
        assertEquals(
            SingleFieldIndex("a_1", Field("a", IndexDirection.UNKNOWN)),
            results.getIndex(this.namespace, "a_1"))
    }
}