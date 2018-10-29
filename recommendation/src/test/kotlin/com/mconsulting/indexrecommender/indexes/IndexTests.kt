package com.mconsulting.indexrecommender.indexes

import com.mconsulting.indexrecommender.readJsonAsBsonDocument
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class IndexTests {

    @Test
    fun _idIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/_id_index.json"))
        assertTrue(index is IdIndex)
    }
}