package com.mconsulting.indexrecommender.indexes

import com.mconsulting.indexrecommender.readJsonAsBsonDocument
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class IndexTests {

    @Test
    fun _idIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/_id_index.json"))
        assertTrue(index is IdIndex)
        assertEquals(true, index.unique)
    }

    @Test
    fun singleFieldIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/single_field_index.json"))
        assertTrue(index is SingleFieldIndex)
        assertEquals(Field("name", IndexDirection.ASCENDING), (index as SingleFieldIndex).field)
        assertEquals(false, index.sparse)
        assertEquals(false, index.unique)
    }

    @Test
    fun compoundIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/compound_field_index.json"))
        assertTrue(index is CompoundIndex)
        assertEquals(listOf(
            Field("name", IndexDirection.ASCENDING),
            Field("text", IndexDirection.DESCENDING)
        ), (index as CompoundIndex).fields)
        assertEquals(false, index.sparse)
        assertEquals(false, index.unique)
    }
}