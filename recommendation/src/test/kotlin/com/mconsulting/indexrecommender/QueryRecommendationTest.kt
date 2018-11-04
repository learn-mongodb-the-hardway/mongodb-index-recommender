package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.indexes.Field
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.indexes.SingleFieldIndex
import com.mconsulting.indexrecommender.profiling.Query
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
        val recommender = IndexRecommendationEngine()
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
        val recommender = IndexRecommendationEngine()
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
        val recommender = IndexRecommendationEngine()
        recommender.add(operation)

        // Return the recommendation
        val recommendation = recommender.recommend()

//        // Validate the indexes
//        assertEquals(1, recommendation.indexes.size)
//        assertEquals(recommendation.indexes[0], Index(listOf(
//            Field("name", IndexDirection.UNKNOWN),
//            Field("number", IndexDirection.UNKNOWN)
//        )))
    }

    @Test
    fun twoLevelFieldsAndSort() {
        val operation = Query(readJsonAsBsonDocument("operations/top_level_two_field_query_with_sort.json"))

        // Create index recommendation engine and add operation
        val recommender = IndexRecommendationEngine()
        recommender.add(operation)

        // Return the recommendation
        val recommendation = recommender.recommend()

//        // Validate the indexes
//        assertEquals(1, recommendation.indexes.size)
//        assertEquals(recommendation.indexes[0], Index(listOf(
//            Field("name", IndexDirection.DESCENDING),
//            Field("number", IndexDirection.ASCENDING)
//        )))
    }

    /**
     * Multikey query
     */
    @Test
    fun multiKeyFieldQuery() {
        val operation = Query(readJsonAsBsonDocument("operations/multi_key_query.json"))

        // Create index recommendation engine and add operation
        val recommender = IndexRecommendationEngine()
        recommender.add(operation)

        // Return the recommendation
        val recommendation = recommender.recommend()
        println()
    }
}