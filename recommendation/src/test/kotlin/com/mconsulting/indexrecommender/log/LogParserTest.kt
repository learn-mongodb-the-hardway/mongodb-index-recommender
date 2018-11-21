package com.mconsulting.indexrecommender.log

import com.mconsulting.indexrecommender.readResourceAsReader
import org.bson.BsonDocument
import org.junit.jupiter.api.Test
import java.io.BufferedReader
import kotlin.test.assertEquals

class LogParserTest {

    @Test
    fun parseQueryAndAggregationLogEntriesFor4_0Test() {
        val reader = readResourceAsReader("logs/query_and_aggregation_log_4_0.txt")
        val logParser = LogParser(BufferedReader(reader))
        val logEntries = mutableListOf<LogEntry>()

        // Go over the log
        logParser.forEach {
            if (it is CommandLogEntry) {
                logEntries += it
            }
        }

        // Grab the commands
        val aggregateCommands = getCommands(logEntries, "aggregate")
        val findCommands = getCommands(logEntries, "find")

        // Assertions
        assertEquals(1, aggregateCommands.size)
        assertEquals(BsonDocument.parse("""
            { "aggregate" : "t", "pipeline" : [{ "${'$'}match" : { } }], "cursor" : { }, "lsid" : { "id" : { "${'$'}binary" : "YjhhNTE1ODgtZmM0ZC00YmZjLTg5ZTQtOTAzYmE3ZmZhZGMx", "${'$'}type" : "04" } }, "${'$'}db" : "mindex_recommendation_tests" }
        """.trimIndent()), (aggregateCommands.first() as CommandLogEntry).command)

        assertEquals(1, findCommands.size)
        assertEquals(BsonDocument.parse("""
            { "find" : "t", "filter" : { "${'$'}text" : { "${'$'}search" : "world" } }, "limit" : 1.0, "singleBatch" : true, "lsid" : { "id" : { "${'$'}binary" : "YjhhNTE1ODgtZmM0ZC00YmZjLTg5ZTQtOTAzYmE3ZmZhZGMx", "${'$'}type" : "04" } }, "${'$'}db" : "mindex_recommendation_tests" }
        """.trimIndent()), (findCommands.first() as CommandLogEntry).command)
    }

    @Test
    fun parseQueryAndAggregationLogEntriesFor3_4Test() {
        val reader = readResourceAsReader("logs/query_and_aggregation_log_3_4.txt")
        val logParser = LogParser(BufferedReader(reader))
        val logEntries = mutableListOf<LogEntry>()

        // Go over the log
        logParser.forEach {
            if (it is CommandLogEntry) {
                logEntries += it
            }
        }

        // Grab the commands
        val aggregateCommands = getCommands(logEntries, "aggregate")
        val findCommands = getCommands(logEntries, "find")

        // Assertions
        assertEquals(1, aggregateCommands.size)
        assertEquals(BsonDocument.parse("""
            { "aggregate" : "t", "pipeline" : [{ "${'$'}match" : { } }], "cursor" : { } }
        """.trimIndent()), (aggregateCommands.first() as CommandLogEntry).command)

        assertEquals(1, findCommands.size)
        assertEquals(BsonDocument.parse("""
            { "find" : "t", "filter" : { "${'$'}text" : { "${'$'}search" : "world" } } }
        """.trimIndent()), (findCommands.first() as CommandLogEntry).command)
    }

    @Test
    fun parseUpdateLogEntriesFor4_0Test() {
        val reader = readResourceAsReader("logs/update_log_4_0.txt")
        val logParser = LogParser(BufferedReader(reader))
        val logEntries = mutableListOf<LogEntry>()

        // Go over the log
        logParser.forEach {
            if (it is CommandLogEntry) {
                logEntries += it
            } else if (it is WriteCommandLogEntry) {
                // Do we have an update operation
                when (it.commandName.toLowerCase()) {
                    "update" -> {
                        logEntries += it
                    }
                }
            } else if (it is NoSupportedLogEntry) {
            }
        }

        val updateCommands = getWrites(logEntries, "update")

        assertEquals(2, updateCommands.size)
        assertEquals(BsonDocument.parse("""
            { "q" : { "a" : 1.0 }, "u" : { "${'$'}set" : { "b" : 1.0 } }, "multi" : false, "upsert" : false }
        """.trimIndent()), (updateCommands[0] as WriteCommandLogEntry).command)
        assertEquals(BsonDocument.parse("""
            { "q" : { "a" : 1.0 }, "u" : { "${'$'}set" : { "b" : 1.0 } }, "multi" : false, "upsert" : false }
        """.trimIndent()), (updateCommands[1] as WriteCommandLogEntry).command)
    }

    @Test
    fun parseUpdateLogEntriesFor3_4Test() {
        val reader = readResourceAsReader("logs/update_log_3_4.txt")
        val logParser = LogParser(BufferedReader(reader))
        val logEntries = mutableListOf<LogEntry>()

        // Go over the log
        logParser.forEach {
            if (it is CommandLogEntry) {
                logEntries += it
            } else if (it is WriteCommandLogEntry) {
                // Do we have an update operation
                when (it.commandName.toLowerCase()) {
                    "update" -> {
                        logEntries += it
                    }
                }
            } else if (it is NoSupportedLogEntry) {
            }
        }

        val updateCommands = getWrites(logEntries, "update")

        assertEquals(1, updateCommands.size)
        assertEquals(BsonDocument.parse("""
            { "q" : { "a" : 1.0 }, "u" : { "${'$'}set" : { "b" : 1.0 } } }
        """.trimIndent()), (updateCommands[0] as WriteCommandLogEntry).command)
    }

    private fun getWrites(logEntries: MutableList<LogEntry>, name: String): List<LogEntry> {
        return logEntries.filter {
            it is WriteCommandLogEntry && it.commandName == name
        }
    }

    private fun getCommands(logEntries: MutableList<LogEntry>, name: String): List<LogEntry> {
        return logEntries.filter {
            it is CommandLogEntry && it.commandName == name
        }
    }
}