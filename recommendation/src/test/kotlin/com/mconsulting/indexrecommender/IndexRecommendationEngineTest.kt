package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.indexes.Field
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.profiling.Query
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class IndexRecommendationEngineTest {

    @Test
    fun singleQueryOperationWithTopLevelField() {
        val operation = Query(readJsonAsBsonDocument("operations/top_level_single_field_query.json"))

        // Create index recommendation engine and add operation
        val recommender = IndexRecommendationEngine()
        recommender.add(operation)

        // Return the recommendation
        val recommendation = recommender.recommend()

        // Validate the indexes
        assertEquals(1, recommendation.indexes.size)
        assertEquals(recommendation.indexes[0], Index(listOf(
            Field("name", IndexDirection.UNKNOWN)
        )))
    }

    @Test
    fun singleQueryOperationWithTopLevelFieldAndSort() {
        val operation = Query(readJsonAsBsonDocument("operations/top_level_single_field_query_with_sort.json"))

        // Create index recommendation engine and add operation
        val recommender = IndexRecommendationEngine()
        recommender.add(operation)

        // Return the recommendation
        val recommendation = recommender.recommend()

        // Validate the indexes
        assertEquals(1, recommendation.indexes.size)
        assertEquals(recommendation.indexes[0], Index(listOf(
            Field("name", IndexDirection.DESCENDING)
        )))
    }

    @Test
    fun singleQueryOperationWithTwoTopLevelFields() {
        val operation = Query(readJsonAsBsonDocument("operations/top_level_two_field_query.json"))

        // Create index recommendation engine and add operation
        val recommender = IndexRecommendationEngine()
        recommender.add(operation)

        // Return the recommendation
        val recommendation = recommender.recommend()

        // Validate the indexes
        assertEquals(1, recommendation.indexes.size)
        assertEquals(recommendation.indexes[0], Index(listOf(
            Field("name", IndexDirection.UNKNOWN),
            Field("number", IndexDirection.UNKNOWN)
        )))
    }

    @Test
    fun singleQueryOperationWithTopTwoLevelFieldsAndSort() {
        val operation = Query(readJsonAsBsonDocument("operations/top_level_two_field_query_with_sort.json"))

        // Create index recommendation engine and add operation
        val recommender = IndexRecommendationEngine()
        recommender.add(operation)

        // Return the recommendation
        val recommendation = recommender.recommend()

        // Validate the indexes
        assertEquals(1, recommendation.indexes.size)
        assertEquals(recommendation.indexes[0], Index(listOf(
            Field("name", IndexDirection.DESCENDING),
            Field("number", IndexDirection.ASCENDING)
        )))
    }
}