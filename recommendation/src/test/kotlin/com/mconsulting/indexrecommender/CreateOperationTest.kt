package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.profiling.Aggregation
import com.mconsulting.indexrecommender.profiling.Delete
import com.mconsulting.indexrecommender.profiling.Insert
import com.mconsulting.indexrecommender.profiling.Query
import com.mconsulting.indexrecommender.profiling.Update
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class CreateOperationTest {
    @Test
    fun parseInsert() {
        val json = readJsonAsJsonDocument("operations/single_row_insert.json")
        val operation = createOperation(json)

        assertNotNull(operation)
        assertTrue(operation is Insert)
        assertEquals(Namespace.parse("digitalvault_integration.users"), operation.namespace())
    }

    @Test
    fun parseAggregation() {
        val json = readJsonAsJsonDocument("operations/single_match_aggregation.json")
        val operation = createOperation(json)

        assertNotNull(operation)
        assertTrue(operation is Aggregation)
        assertEquals(Namespace.parse("digitalvault_integration.users"), operation.namespace())
    }

    @Test
    fun parseUpdate() {
        val json = readJsonAsJsonDocument("operations/update.json")
        val operation = createOperation(json)

        assertNotNull(operation)
        assertTrue(operation is Update)
        assertEquals(Namespace.parse("digitalvault_integration.users"), operation.namespace())
    }

    @Test
    fun parseQuery() {
        val json = readJsonAsJsonDocument("operations/multi_key_query.json")
        val operation = createOperation(json)

        assertNotNull(operation)
        assertTrue(operation is Query)
        assertEquals(Namespace.parse("mindex_recommendation_tests.gamers"), operation.namespace())
    }

    @Test
    fun parseDelete() {
        val json = readJsonAsJsonDocument("operations/delete_one.json")
        val operation = createOperation(json)

        assertNotNull(operation)
        assertTrue(operation is Delete)
        assertEquals(Namespace.parse("digitalvault_integration.users"), operation.namespace())
    }
}