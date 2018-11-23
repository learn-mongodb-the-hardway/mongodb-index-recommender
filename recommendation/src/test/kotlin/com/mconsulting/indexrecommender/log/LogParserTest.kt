package com.mconsulting.indexrecommender.log

import com.mconsulting.indexrecommender.readResourceAsReader
import org.bson.BsonDocument
import org.bson.BsonInt64
import org.bson.BsonJavaScript
import org.bson.BsonRegularExpression
import org.bson.BsonString
import org.junit.jupiter.api.Test
import java.io.BufferedReader
import java.io.StringReader
import kotlin.test.assertEquals
import kotlin.test.assertTrue

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

    @Test
    fun parseRemoveLogEntriesFor4_0Test() {
        val reader = readResourceAsReader("logs/remove_log_4_0.txt")
        val logParser = LogParser(BufferedReader(reader))
        val logEntries = mutableListOf<LogEntry>()

        // Go over the log
        logParser.forEach {
            if (it is CommandLogEntry) {
                logEntries += it
            } else if (it is WriteCommandLogEntry) {
                // Do we have an update operation
                when (it.commandName.toLowerCase()) {
                    "remove" -> {
                        logEntries += it
                    }
                }
            } else if (it is NoSupportedLogEntry) {
            }
        }

        val deleteCommands = getWrites(logEntries, "remove")

        assertEquals(1, deleteCommands.size)
        assertEquals(BsonDocument.parse("""
            { "q" : { "a" : 1.0 }, "limit" : 1 }
        """.trimIndent()), (deleteCommands[0] as WriteCommandLogEntry).command)
    }

    @Test
    fun parseRemoveLogEntriesFor3_4Test() {
        val reader = readResourceAsReader("logs/remove_log_3_4.txt")
        val logParser = LogParser(BufferedReader(reader))
        val logEntries = mutableListOf<LogEntry>()

        // Go over the log
        logParser.forEach {
            if (it is CommandLogEntry) {
                logEntries += it
            } else if (it is WriteCommandLogEntry) {
                // Do we have an update operation
                when (it.commandName.toLowerCase()) {
                    "remove" -> {
                        logEntries += it
                    }
                }
            } else if (it is NoSupportedLogEntry) {
            }
        }

        val deleteCommands = getWrites(logEntries, "remove")

        assertEquals(1, deleteCommands.size)
        assertEquals(BsonDocument.parse("""
            { "q" : { "a" : 1.0 } }
        """.trimIndent()), (deleteCommands[0] as WriteCommandLogEntry).command)
    }

    @Test
    fun parseEdgeCasesCorrectly() {
        val reader = readResourceAsReader("logs/edge_cases_log.txt")
        val logParser = LogParser(BufferedReader(reader))
        val logEntries = mutableListOf<LogEntry>()

        // Go over the log
        logParser.forEach {
            logEntries += it
        }

        assertEquals(4, logEntries.size)
        assertEquals(BsonDocument.parse("""
            { "q" : { "a" : 1.0, "command": 2 }, "u" : { "${'$'}set" : { "b" : 1.0 } }, "multi" : false, "upsert" : false }
        """.trimIndent()), (logEntries[0] as WriteCommandLogEntry).command)
        assertEquals(BsonDocument.parse("""
            { "q" : { "a" : 1.0, "update": 2 }, "u" : { "${'$'}set" : { "b" : 1.0 } }, "multi" : false, "upsert" : false }
        """.trimIndent()), (logEntries[1] as WriteCommandLogEntry).command)
        assertEquals(BsonDocument.parse("""
            { "q" : { "a" : 1.0, "update" : 2.0 }, "u" : { "${'$'}set" : { "b" : 1.0, "update" : 2.0 } } }
        """.trimIndent()), (logEntries[2] as WriteCommandLogEntry).command)
        assertEquals(BsonDocument.parse("""
            { "q" : { "a" : 1.0, "update" : 2.0 }, "u" : { "${'$'}set" : { "b" : 1.0, "update" : 2.0 } } }
        """.trimIndent()), (logEntries[3] as WriteCommandLogEntry).command)
        assertEquals(10, (logEntries[3] as WriteCommandLogEntry).keysExamined)
    }

    @Test
    fun parseLogLineOverMultipleLines() {
        val logEntriesText = """
            |2018-11-22T17:00:20.456+0100 I COMMAND  [conn620] command test.max_time_ms appName: "MongoDB Shell" command: find { find: "max_time_ms", filter: { ${'$'}where: function () {
        |sleep(100);
        |return true;
    |} }, maxTimeMS: 10000.0 } planSummary: COLLSCAN keysExamined:0 docsExamined:3 cursorExhausted:1 numYields:3 nreturned:3 reslen:164 locks:{ Global: { acquireCount: { r: 8 } }, Database: { acquireCount: { r: 4 } }, Collection: { acquireCount: { r: 4 } } } protocol:op_command 313ms
            |2018-11-22T17:00:20.456+0100 I COMMAND  [conn620] CMD: drop test.max_time_ms
        """.trimMargin("|")

        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(2, logEntries.size)
        assertTrue(logEntries.first() is CommandLogEntry)
        assertEquals(BsonDocument()
            .append("find", BsonString("max_time_ms"))
            .append("filter", BsonDocument()
                .append("${'$'}where", BsonJavaScript("function () { sleep(100); return true; }")))
            .append("maxTimeMS", BsonInt64(10000)), (logEntries.first() as CommandLogEntry).command)
    }

    @Test
    fun parseLogLineOverMultipleLinesEOF() {
        val logEntriesText = """
            2018-11-22T17:00:20.456+0100 I COMMAND  [conn620] command test.max_time_ms appName: "MongoDB Shell" command: find { find: "max_time_ms", filter: { ${'$'}where: function () {
        sleep(100);
        return true;
    } }, maxTimeMS: 10000.0 } planSummary: COLLSCAN keysExamined:0 docsExamined:3 cursorExhausted:1 numYields:3 nreturned:3 reslen:164 locks:{ Global: { acquireCount: { r: 8 } }, Database: { acquireCount: { r: 4 } }, Collection: { acquireCount: { r: 4 } } } protocol:op_command 313ms""".trimIndent()

        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertTrue(logEntries.first() is CommandLogEntry)
        assertEquals(BsonDocument()
            .append("find", BsonString("max_time_ms"))
            .append("filter", BsonDocument()
                .append("${'$'}where", BsonJavaScript("function () { sleep(100); return true; }")))
            .append("maxTimeMS", BsonInt64(10000)), (logEntries.first() as CommandLogEntry).command)
    }

    @Test
    fun parseLogLineWithFunctionAndRegularExpression() {
        val logEntriesText = """
            2018-11-22T17:00:20.456+0100 I COMMAND  [conn620] command test.max_time_ms appName: "MongoDB Shell" command: find { find: "max_time_ms", filter: { a: /ad/, ${'$'}where: function () {
        sleep(100);
        return true;
    } }, maxTimeMS: 10000.0 } planSummary: COLLSCAN keysExamined:0 docsExamined:3 cursorExhausted:1 numYields:3 nreturned:3 reslen:164 locks:{ Global: { acquireCount: { r: 8 } }, Database: { acquireCount: { r: 4 } }, Collection: { acquireCount: { r: 4 } } } protocol:op_command 313ms""".trimIndent()

        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertTrue(logEntries.first() is CommandLogEntry)
        assertEquals(BsonDocument()
            .append("find", BsonString("max_time_ms"))
            .append("filter", BsonDocument()
                .append("a", BsonRegularExpression("ad", ""))
                .append("${'$'}where", BsonJavaScript("function () { sleep(100); return true; }")))
            .append("maxTimeMS", BsonInt64(10000)), (logEntries.first() as CommandLogEntry).command)
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