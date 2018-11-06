package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.profiling.Query
import org.bson.BsonDateTime
import org.bson.Document
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.test.assertEquals

class StatisticsProcessorTest {

    @Test
    fun shouldAddBasicQueryToStatistics() {
        val statisticsProcessor = StatisticsProcessor()
        statisticsProcessor.process(Query(toBsonDocument(Document(mapOf(
            "command" to mapOf(
                "\$db" to "test",
                "find" to "entries",
                "filter" to mapOf(
                    "a" to 10,
                    "b" to "hello world",
                    "c" to mapOf(
                        "d" to 10.1
                    )
                )
            ),
            "ts" to BsonDateTime(Date().time)
        )))))

        assertEquals(1, statisticsProcessor.shapes.size)
        assertEquals(toBsonDocument(Document(mapOf(
            "a" to true,
            "b" to true,
            "c" to mapOf(
                "d" to true
            )
        ))), statisticsProcessor.shapes.first().shape)
    }

    @Test
    fun shouldUpdateExistingStatisticsEntry() {
        val statisticsProcessor = StatisticsProcessor()
        statisticsProcessor.process(Query(toBsonDocument(Document(mapOf(
            "command" to mapOf(
                "\$db" to "test",
                "find" to "entries",
                "filter" to mapOf(
                    "a" to 10,
                    "b" to "hello world",
                    "c" to mapOf(
                        "d" to 10.1
                    )
                )
            ),
            "ts" to BsonDateTime(Date().time - 2000)
        )))))

        statisticsProcessor.process(Query(toBsonDocument(Document(mapOf(
            "command" to mapOf(
                "\$db" to "test",
                "find" to "entries",
                "filter" to mapOf(
                    "a" to 10,
                    "b" to "hello world",
                    "c" to mapOf(
                        "d" to 10
                    )
                )
            ),
            "ts" to BsonDateTime(Date().time)
        )))))

        assertEquals(1, statisticsProcessor.shapes.size)
        assertEquals(toBsonDocument(Document(mapOf(
            "a" to true,
            "b" to true,
            "c" to mapOf(
                "d" to true
            )
        ))), statisticsProcessor.shapes.first().shape)
        assertEquals(2, statisticsProcessor.shapes.first().count)
        assertEquals(2, statisticsProcessor.shapes.first().frequency.size)
        assertEquals(1, statisticsProcessor.shapes.first().frequency.values.first().count)
        assertEquals(1, statisticsProcessor.shapes.last().frequency.values.first().count)
    }
}