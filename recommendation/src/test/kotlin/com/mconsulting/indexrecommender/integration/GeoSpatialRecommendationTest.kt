package com.mconsulting.indexrecommender.integration

import com.mconsulting.indexrecommender.indexes.Field
import com.mconsulting.indexrecommender.indexes.IdIndex
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.indexes.MultikeyIndex
import org.bson.Document
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class GeoSpatialRecommendationTest : IntegrationTestBase() {

    @Test
    fun simpleGeoSpatialRecommendation() {
//        // Setup the case
//        executeOperations {
//            // Insert a document
//            collection.insertOne(Document(mapOf(
//                "location" to mapOf(
//                    "type" to "Point",
//                    "coordinates" to listOf(-73.856077, 40.848447)
//                )
//            )))
//
//            // Execute a query (expect us to get one)
//            val doc = collection.find(Document(mapOf(
//                "location" to mapOf(
//                    "\$near" to mapOf(
//                        "\$geometry" to mapOf(
//                            "type" to "Point",
//                            "coordinates" to listOf(-73.856077, 40.848447)
//                        ),
//                        "\$maxDistance" to 100
//                    )
//                )
//            ))).first()
//            println()
//        }
//
//        // Run the recommendation engine
//        val results = runRecommendationEngine()
//
//        // Validate that we have the expected indexes
//        assertEquals(2, results.indexes.size)
//        assertEquals(
//            IdIndex("_id_"),
//            results.getIndex("_id_"))
//        assertEquals(
//            MultikeyIndex("a_1_b.c_1", listOf(
//                Field("a", IndexDirection.UNKNOWN),
//                Field("b.c", IndexDirection.UNKNOWN)
//            )),
//            results.getIndex("a_1_b.c_1"))
    }
}