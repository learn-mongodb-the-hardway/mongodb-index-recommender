package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.profiling.Query
import org.junit.jupiter.api.Test

class IndexRecommendationEngineTest {

    @Test
    fun singleQueryOperationWithTopLevelField() {
        val operation = Query(readJsonAsBsonDocument("operations/top_level_single_field_query.json"))

        // Create index recommendation engine and add operation
        val recommender = IndexRecommendationEngine()
        recommender.add(operation)

        // Return the recommendation
        val recommendation = recommender.recommend()
        println()
    }
}