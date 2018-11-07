package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.profiling.Aggregation
import com.mconsulting.indexrecommender.profiling.Delete
import com.mconsulting.indexrecommender.profiling.Insert
import com.mconsulting.indexrecommender.profiling.Query
import com.mconsulting.indexrecommender.profiling.Update
import org.bson.BsonArray
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
            "ns" to "digitalvault_integration.users",
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
            "ts" to BsonDateTime(Date().time),
            "responseLength" to 450,
            "millis" to 25,
            "nreturned" to 10
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
        val statisticsProcessor = StatisticsProcessor(StatisticsProcessorOptions(
            bucketResolution = TimeResolution.HOUR
        ))

        statisticsProcessor.process(Query(toBsonDocument(Document(mapOf(
            "ns" to "digitalvault_integration.users",
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
            "ts" to BsonDateTime(Date().time - 2000),
            "responseLength" to 150,
            "millis" to 15,
            "nreturned" to 10
        )))))

        statisticsProcessor.process(Query(toBsonDocument(Document(mapOf(
            "ns" to "digitalvault_integration.users",
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
            "ts" to BsonDateTime(Date().time),
            "responseLength" to 450,
            "millis" to 25,
            "nreturned" to 20
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
        assertEquals(1, statisticsProcessor.shapes.first().frequency.size)
        assertEquals(2, statisticsProcessor.shapes.first().frequency.values.first().count)
    }

    @Test
    fun shouldAddBasicAggregationToStatistics() {
        val statisticsProcessor = StatisticsProcessor()
        statisticsProcessor.process(Aggregation(toBsonDocument(Document(mapOf(
            "ns" to "digitalvault_integration.users",
            "command" to mapOf(
                "\$db" to "test",
                "aggregate" to "entries",
                "pipeline" to listOf(
                    mapOf(
                        "\$match" to Document()
                    )
                )
            ),
            "ts" to BsonDateTime(Date().time),
            "responseLength" to 450,
            "millis" to 25,
            "nreturned" to 10
        )))))

        statisticsProcessor.process(Aggregation(toBsonDocument(Document(mapOf(
            "ns" to "digitalvault_integration.users",
            "command" to mapOf(
                "\$db" to "test",
                "aggregate" to "entries",
                "pipeline" to listOf(
                    mapOf(
                        "\$match" to Document()
                    )
                )
            ),
            "ts" to BsonDateTime(Date().time),
            "responseLength" to 450,
            "millis" to 25,
            "nreturned" to 10
        )))))

        assertEquals(1, statisticsProcessor.shapes.size)
        assertEquals(BsonArray(mutableListOf(
            toBsonDocument(Document(mapOf(
                "\$match" to Document()
            )))
        )), statisticsProcessor.shapes.first().shape)
    }

    @Test
    fun shouldAddBasicInsertToStatistics() {
        val statisticsProcessor = StatisticsProcessor()
        statisticsProcessor.process(Insert(readJsonAsBsonDocument("operations/single_row_insert.json")))

        assertEquals(1, statisticsProcessor.inserts!!.frequency.size)
        assertEquals(1, statisticsProcessor.inserts!!.frequency.values.first().count)
    }

    @Test
    fun shouldAddBasicUpdateToStatistics() {
        val statisticsProcessor = StatisticsProcessor()
        statisticsProcessor.process(Update(readJsonAsBsonDocument("operations/update.json")))

        assertEquals(1, statisticsProcessor.shapes.size)
        assertEquals(toBsonDocument(Document(mapOf(
            "_id" to true,
            "name" to true
        ))), statisticsProcessor.shapes.first().shape)
    }

    @Test
    fun shouldAddBasicDeleteToStatistics() {
        val statisticsProcessor = StatisticsProcessor()
        statisticsProcessor.process(Delete(readJsonAsBsonDocument("operations/delete_one.json")))

        assertEquals(1, statisticsProcessor.shapes.size)
        assertEquals(toBsonDocument(Document(mapOf(
            "name" to true
        ))), statisticsProcessor.shapes.first().shape)
    }
}