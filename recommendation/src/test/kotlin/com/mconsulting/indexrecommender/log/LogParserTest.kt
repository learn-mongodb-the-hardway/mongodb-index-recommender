package com.mconsulting.indexrecommender.log

import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import com.mconsulting.indexrecommender.readResourceAsReader
import org.bson.BsonDocument
import org.bson.BsonInt64
import org.bson.BsonJavaScript
import org.bson.BsonNull
import org.bson.BsonString
import org.bson.BsonUndefined
import org.junit.jupiter.api.Test
import java.io.BufferedReader
import java.io.StringReader
import kotlin.test.assertEquals
import kotlin.test.assertTrue

fun parseJSON(json: String) : JsonObject {
    return Parser().parse(StringReader(json)) as JsonObject
}

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
        assertEquals(parseJSON("""
            { "aggregate" : "t", "pipeline" : [{ "${'$'}match" : { } }], "cursor" : { }, "lsid" : { "id" : { "${'$'}binary" : "YjhhNTE1ODgtZmM0ZC00YmZjLTg5ZTQtOTAzYmE3ZmZhZGMx", "${'$'}type" : "04" } }, "${'$'}db" : "mindex_recommendation_tests" }
        """.trimIndent()), (aggregateCommands.first() as CommandLogEntry).command)

        assertEquals(1, findCommands.size)
        assertEquals(parseJSON("""
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
        assertEquals(parseJSON("""
            { "aggregate" : "t", "pipeline" : [{ "${'$'}match" : { } }], "cursor" : { } }
        """.trimIndent()), (aggregateCommands.first() as CommandLogEntry).command)

        assertEquals(1, findCommands.size)
        assertEquals(parseJSON("""
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
        assertEquals(parseJSON("""
            { "q" : { "a" : 1.0 }, "u" : { "${'$'}set" : { "b" : 1.0 } }, "multi" : false, "upsert" : false }
        """.trimIndent()), (updateCommands[0] as WriteCommandLogEntry).command)
        assertEquals(parseJSON("""
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
        assertEquals(parseJSON("""
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
        assertEquals(parseJSON("""
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
        assertEquals(parseJSON("""
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
        assertEquals(parseJSON("""
            { "q" : { "a" : 1.0, "command": 2 }, "u" : { "${'$'}set" : { "b" : 1.0 } }, "multi" : false, "upsert" : false }
        """.trimIndent()), (logEntries[0] as WriteCommandLogEntry).command)
        assertEquals(parseJSON("""
            { "q" : { "a" : 1.0, "update": 2 }, "u" : { "${'$'}set" : { "b" : 1.0 } }, "multi" : false, "upsert" : false }
        """.trimIndent()), (logEntries[1] as WriteCommandLogEntry).command)
        assertEquals(parseJSON("""
            { "q" : { "a" : 1.0, "update" : 2.0 }, "u" : { "${'$'}set" : { "b" : 1.0, "update" : 2.0 } } }
        """.trimIndent()), (logEntries[2] as WriteCommandLogEntry).command)
        assertEquals(parseJSON("""
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
        assertEquals(parseJSON("""
            {
                "find": "max_time_ms",
                "filter": {
                    "${'$'}where": { "${'$'}code": "function () { sleep(100); return true; }" }
                },
                "maxTimeMS": { "${'$'}numberLong": "10000" }
            }""".trimMargin()),
            (logEntries.first() as CommandLogEntry).command)
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
        assertEquals(parseJSON("""
            {
                "find": "max_time_ms",
                "filter": {
                    "${'$'}where": { "${'$'}code": "function () { sleep(100); return true; }" }
                },
                "maxTimeMS": { "${'$'}numberLong": "10000" }
            }""".trimMargin()),
            (logEntries.first() as CommandLogEntry).command)
    }

    @Test
    fun shouldCorrectlyRecoverDueToMallformedJSFunctionShorteningByMongoDB() {
        val logEntriesText = """2018-11-22T17:00:20.484+0100 I COMMAND  [conn620] command test.max_time_ms appName: "MongoDB Shell" command: find { find: "max_time_ms", filter: { ${'$'}where: function () {
                  if (this.slow) {
                     ... }, batchSize: 3.0, sort: { _id: 1.0 }, maxTimeMS: 4000.0 } planSummary: IXSCAN { _id: 1 } cursorid:11501599070999 keysExamined:3 docsExamined:3 numYields:0 nreturned:3 reslen:152 locks:{ Global: { acquireCount: { r: 2 } }, Database: { acquireCount: { r: 1 } }, Collection: { acquireCount: { r: 1 } } } protocol:op_command 0ms
2018-11-22T17:00:22.773+0100 I COMMAND  [conn171] command test.system.profile appName: "MongoDB Shell" command: collStats { collStats: "system.profile", scale: undefined } numYields:0 reslen:5737 locks:{ Global: { acquireCount: { r: 2 } }, Database: { acquireCount: { r: 1 } }, Collection: { acquireCount: { r: 1 } } } protocol:op_command 0ms"""

        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertTrue(logEntries.first() is CommandLogEntry)
        assertEquals(parseJSON("""
            {
                "collStats": "system.profile",
                "scale": { "${'$'}undefined": true }
            }""".trimMargin()),
            (logEntries.first() as CommandLogEntry).command)
    }

    @Test
    fun shouldCorrectlyParseIndexDefWithPath() {
        val logEntriesText = """2018-11-22T17:01:28.322+0100 I COMMAND  [conn678] command test.null2 appName: "MongoDB Shell" command: find { find: "null2", filter: { a.b: null } } planSummary: IXSCAN { a.b: 1 } keysExamined:4 docsExamined:4 cursorExhausted:1 numYields:0 nreturned:2 reslen:176 locks:{ Global: { acquireCount: { r: 2 } }, Database: { acquireCount: { r: 1 } }, Collection: { acquireCount: { r: 1 } } } protocol:op_command 0ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertTrue(logEntries.first() is CommandLogEntry)
        assertEquals(parseJSON("""
            {
                "find": "null2",
                "filter": {
                    "a.b": null
                }
            }""".trimMargin()),
            (logEntries.first() as CommandLogEntry).command)
    }

    @Test
    fun shouldCorrectlHandleInfinitySign() {
        val logEntriesText = """2018-11-22T17:05:25.515+0100 I COMMAND  [conn1094] command expression_mod.test appName: "MongoDB Shell" command: aggregate { aggregate: "test", pipeline: [ { ${'$'}project: { output: { ${'$'}mod: [ -inf.0, 10.0 ] } } } ], cursor: {} } planSummary: COLLSCAN keysExamined:0 docsExamined:1 cursorExhausted:1 numYields:0 nreturned:1 reslen:133 locks:{ Global: { acquireCount: { r: 8 } }, Database: { acquireCount: { r: 4 } }, Collection: { acquireCount: { r: 3 } } } protocol:op_command 0m"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertEquals(
            parseJSON("""{ "aggregate" : "test", "pipeline" : [{ "${'$'}project" : { "output" : { "${'$'}mod" : ["-inf.0", 10.0] } } }], "cursor" : { } }"""),
            (logEntries.first() as CommandLogEntry).command)
    }

    @Test
    fun shouldCorrectlHandleNaNSign() {
        val logEntriesText = """2018-11-22T17:05:25.515+0100 I COMMAND  [conn1094] command expression_mod.test appName: "MongoDB Shell" command: aggregate { aggregate: "test", pipeline: [ { ${'$'}project: { output: { ${'$'}mod: [ nan.0, 10.0 ] } } } ], cursor: {} } planSummary: COLLSCAN keysExamined:0 docsExamined:1 cursorExhausted:1 numYields:0 nreturned:1 reslen:133 locks:{ Global: { acquireCount: { r: 8 } }, Database: { acquireCount: { r: 4 } }, Collection: { acquireCount: { r: 3 } } } protocol:op_command 0m"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertEquals(
            parseJSON("""{ "aggregate" : "test", "pipeline" : [{ "${'$'}project" : { "output" : { "${'$'}mod" : ["nan.0", 10.0] } } }], "cursor" : { } }"""),
            (logEntries.first() as CommandLogEntry).command)
    }

    @Test
    fun shouldHandleInsertCommandCorrectly() {
        val logEntriesText = """2018-11-22T17:05:24.999+0100 I COMMAND  [conn1092] command test.array_to_object_expr appName: "MongoDB Shell" command: insert { insert: "array_to_object_expr", documents: [ { _id: 23.0, expanded: [ { k: undefined, v: "undefinedKey" } ] } ], ordered: true } ninserted:1 keysInserted:1 numYields:0 reslen:29 locks:{ Global: { acquireCount: { r: 1, w: 1 } }, Database: { acquireCount: { w: 1 } }, Collection: { acquireCount: { w: 1 } } } protocol:op_command 0ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertEquals(parseJSON(
            """{ "insert" : "array_to_object_expr", "documents" : [{ "_id" : 23.0, "expanded" : [{ "k" : { "${'$'}undefined" : true }, "v" : "undefinedKey" }] }], "ordered" : true }"""),
            (logEntries.first() as CommandLogEntry).command)
    }

    @Test
    fun shouldCorrectlyParseDate() {
        val logEntriesText = """2018-11-22T17:05:24.369+0100 I COMMAND  [conn1088] command test.jstests_aggregation_strcasecmp appName: "MongoDB Shell" command: aggregate { aggregate: "jstests_aggregation_strcasecmp", pipeline: [ { ${'$'}project: { a: { ${'$'}strcasecmp: [ new Date(0), "1970-01-01t00:00:00" ] } } } ], cursor: {} } planSummary: COLLSCAN keysExamined:0 docsExamined:1 cursorExhausted:1 numYields:0 nreturned:1 reslen:140 locks:{ Global: { acquireCount: { r: 8 } }, Database: { acquireCount: { r: 4 } }, Collection: { acquireCount: { r: 3 } } } protocol:op_command 0ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertEquals(
            parseJSON("""{ "aggregate" : "jstests_aggregation_strcasecmp", "pipeline" : [{ "${'$'}project" : { "a" : { "${'$'}strcasecmp" : [{ "${'$'}numberLong" : "0" }, "1970-01-01t00:00:00"] } } }], "cursor" : { } }"""),
            (logEntries.first() as CommandLogEntry).command
        )
    }

    @Test
    fun shouldCorrectlyParseExceptionInLogStatement() {
        // TODO Fix Test
        val logEntriesText = """2018-11-22T17:05:15.052+0100 I COMMAND  [conn1067] command test.jstests_aggregation_server6290 appName: "MongoDB Shell" command: aggregate { aggregate: "jstests_aggregation_server6290", pipeline: [ { ${'$'}group: { _id: 0.0, a: { ${'$'}first: { ${'$'}date: [ { year: 1.0 } ] } } } } ], cursor: { batchSize: 0.0 } } exception: Unrecognized expression '${'$'}date' code:168 numYields:0 reslen:114 locks:{ Global: { acquireCount: { r: 4 } }, Database: { acquireCount: { r: 2 } }, Collection: { acquireCount: { r: 1 } } } protocol:op_command 0ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

//        logParser.forEach {
//            logEntries += it
//        }

//        assertEquals(1, logEntries.size)
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