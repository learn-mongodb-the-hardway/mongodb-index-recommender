package com.mconsulting.indexrecommender.integration

import com.mconsulting.indexrecommender.Processor
import com.mconsulting.indexrecommender.indexes.IdIndex
import com.mconsulting.indexrecommender.indexes.TwoDSphereIndex
import com.mconsulting.indexrecommender.ingress.ProfileCollectionIngress
import org.bson.Document
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class GeoSpatialRecommendationTest : IntegrationTestBase() {

    @Test
    fun simpleGeoSpatialRecommendation() {
        // Setup the case
        executeOperations {
            // Insert a document
            collection.insertOne(Document(mapOf(
                "location" to mapOf(
                    "type" to "Point",
                    "coordinates" to listOf(-73.856077, 40.848447)
                )
            )))

            //{ location: { $geoWithin: { $center: [ [ -73.856077, 40.848447 ], 10 ] } } }
            // Execute a query (expect us to get one)
            val doc = collection.find(Document(mapOf(
                "location" to mapOf(
                    "\$geoWithin" to mapOf(
                        "\$center" to listOf(listOf(-73.856077, 40.848447), 100)
                    )
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
        assertEquals(2, indexes.size)
        assertEquals(
            IdIndex("_id_"),
            results.getIndex(this.namespace, "_id_"))
        assertEquals(
            TwoDSphereIndex("location_1", "location"),
            results.getIndex(this.namespace, "location_1"))
    }
}