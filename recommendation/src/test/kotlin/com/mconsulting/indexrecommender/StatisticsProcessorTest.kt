package com.mconsulting.indexrecommender

import com.beust.klaxon.JsonArray
import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import com.mconsulting.indexrecommender.profiling.Aggregation
import com.mconsulting.indexrecommender.profiling.Delete
import com.mconsulting.indexrecommender.profiling.Insert
import com.mconsulting.indexrecommender.profiling.Query
import com.mconsulting.indexrecommender.profiling.Update
import org.bson.BsonDateTime
import org.bson.Document
import org.junit.jupiter.api.Test
import java.io.StringReader
import java.util.*
import kotlin.test.assertEquals

class StatisticsProcessorTest {

    @Test
    fun shouldAddBasicQueryToStatistics() {
        val statisticsProcessor = StatisticsProcessor()
        statisticsProcessor.process(Query(Parser().parse(StringReader(Document(mapOf(
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
        )).toJson())) as JsonObject))

        assertEquals(1, statisticsProcessor.shapes.size)
        assertEquals(Parser().parse(StringReader(Document(mapOf(
            "a" to true,
            "b" to true,
            "c" to mapOf(
                "d" to true
            )
        )).toJson())), statisticsProcessor.shapes.first().shape)
    }

    @Test
    fun shouldUpdateExistingStatisticsEntry() {
        val statisticsProcessor = StatisticsProcessor(StatisticsProcessorOptions(
            bucketResolution = TimeResolution.HOUR
        ))

        statisticsProcessor.process(Query(Parser().parse(StringReader(Document(mapOf(
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
        )).toJson())) as JsonObject))

        statisticsProcessor.process(Query(Parser().parse(StringReader(Document(mapOf(
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
        )).toJson())) as JsonObject))

        assertEquals(1, statisticsProcessor.shapes.size)
        assertEquals(Parser().parse(StringReader(Document(mapOf(
            "a" to true,
            "b" to true,
            "c" to mapOf(
                "d" to true
            )
        )).toJson())), statisticsProcessor.shapes.first().shape)
        assertEquals(2, statisticsProcessor.shapes.first().count)
        assertEquals(1, statisticsProcessor.shapes.first().frequency.size)
        assertEquals(2, statisticsProcessor.shapes.first().frequency.values.first().count)
    }

    @Test
    fun shouldAddBasicAggregationToStatistics() {
        val statisticsProcessor = StatisticsProcessor()
        statisticsProcessor.process(Aggregation(Parser().parse(StringReader(Document(mapOf(
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
        )).toJson())) as JsonObject))


        statisticsProcessor.process(Aggregation(Parser().parse(StringReader(Document(mapOf(
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
        )).toJson())) as JsonObject))

        assertEquals(1, statisticsProcessor.shapes.size)
        assertEquals(JsonArray(mutableListOf(
            JsonObject(mapOf(
                "\$match" to JsonObject()
            ))
        )), statisticsProcessor.shapes.first().shape)
    }

    @Test
    fun shouldAddBasicInsertToStatistics() {
        val statisticsProcessor = StatisticsProcessor()
        statisticsProcessor.process(Insert(readJsonAsJsonDocument("operations/single_row_insert.json")))

        assertEquals(1, statisticsProcessor.inserts!!.frequency.size)
        assertEquals(1, statisticsProcessor.inserts!!.frequency.values.first().count)
    }

    @Test
    fun shouldAddBasicUpdateToStatistics() {
        val statisticsProcessor = StatisticsProcessor()
        statisticsProcessor.process(Update(readJsonAsJsonDocument("operations/update.json")))

        assertEquals(1, statisticsProcessor.shapes.size)
        assertEquals(JsonObject(mapOf(
            "_id" to true,
            "name" to true
        )), statisticsProcessor.shapes.first().shape)
    }

    @Test
    fun shouldAddBasicDeleteToStatistics() {
        val statisticsProcessor = StatisticsProcessor()
        statisticsProcessor.process(Delete(readJsonAsJsonDocument("operations/delete_one.json")))

        assertEquals(1, statisticsProcessor.shapes.size)
        assertEquals(JsonObject(mapOf(
            "name" to true
        )), statisticsProcessor.shapes.first().shape)
    }
}