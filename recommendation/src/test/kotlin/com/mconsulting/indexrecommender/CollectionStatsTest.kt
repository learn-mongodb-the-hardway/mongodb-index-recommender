package com.mconsulting.indexrecommender

import org.bson.BsonDocument
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class CollectionStatsTest {

    @Test
    fun shouldCorrectlyCreateStats() {
        val document = readJsonAsBsonDocument("stats/collection_stats.json")
        val stats = CollectionStats(document)

        assertEquals(5, stats.inserts)
        assertEquals(25, stats.finds)
        assertEquals(0, stats.removes)
        assertEquals(0, stats.updates)
        assertEquals(5, stats.count)
        assertEquals(5.0, stats.readWriteRatio)
    }

    @Test
    fun shouldCorrectlyHandleWrongDoc1() {
        val stats = CollectionStats(BsonDocument())

        assertEquals(0, stats.inserts)
        assertEquals(0, stats.finds)
        assertEquals(0, stats.removes)
        assertEquals(0, stats.updates)
        assertEquals(0, stats.count)
        assertEquals(0.0, stats.readWriteRatio)
    }

    @Test
    fun shouldCorrectlyHandleWrongDoc2() {
        val stats = CollectionStats(BsonDocument()
            .append("wiredTiger", BsonDocument()))

        assertEquals(0, stats.inserts)
        assertEquals(0, stats.finds)
        assertEquals(0, stats.removes)
        assertEquals(0, stats.updates)
        assertEquals(0, stats.count)
        assertEquals(0.0, stats.readWriteRatio)
    }
}