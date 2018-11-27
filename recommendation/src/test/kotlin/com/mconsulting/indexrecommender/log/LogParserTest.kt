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
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
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
            {"find":"max_time_ms","filter":{"${'$'}where":{"${'$'}code":"function () {\nsleep(100);\nreturn true;\n}"}},"maxTimeMS":10000.0}
        """.trimMargin()),
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
            {"find":"max_time_ms","filter":{"${'$'}where":{"${'$'}code":"function () {\n sleep(100);\n return true;\n}"}},"maxTimeMS":10000.0}
        """.trimMargin()),
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

        assertEquals(2, logEntries.size)
        assertTrue(logEntries[0] is NoSupportedLogEntry)
        assertTrue(logEntries[1] is CommandLogEntry)
        assertEquals(parseJSON("""
            {
                "collStats": "system.profile",
                "scale": { "${'$'}undefined": true }
            }""".trimMargin()),
            (logEntries[1] as CommandLogEntry).command)
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
    fun shouldCorrectlyParseObjectId() {
        val logEntriesText = """2018-11-22T17:00:18.957+0100 I COMMAND  [conn617] command test.long_index_rename appName: "MongoDB Shell" command: insert { insert: "long_index_rename", documents: [ { a: 1.0, _id: ObjectId('5bf6d29201fd9c2e3958e4ec') } ], ordered: true } ninserted:1 keysInserted:1 numYields:0 reslen:29 locks:{ Global: { acquireCount: { r: 3, w: 3 } }, Database: { acquireCount: { w: 2, W: 1 } }, Collection: { acquireCount: { w: 2 } } } protocol:op_command 19ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertEquals(
            parseJSON("""{"insert":"long_index_rename","documents":[{"a":1.0,"_id":{"${'$'}oid":"5bf6d29201fd9c2e3958e4ec"}}],"ordered":true}"""),
            (logEntries.first() as CommandLogEntry).command
        )
    }

    @Test
    fun shouldCorrectlyParseExceptionInLogStatement() {
        val logEntriesText = """2018-11-22T17:05:15.052+0100 I COMMAND  [conn1067] command test.jstests_aggregation_server6290 appName: "MongoDB Shell" command: aggregate { aggregate: "jstests_aggregation_server6290", pipeline: [ { ${'$'}group: { _id: 0.0, a: { ${'$'}first: { ${'$'}date: [ { year: 1.0 } ] } } } } ], cursor: { batchSize: 0.0 } } exception: Unrecognized expression '${'$'}date' code:168 numYields:0 reslen:114 locks:{ Global: { acquireCount: { r: 4 } }, Database: { acquireCount: { r: 2 } }, Collection: { acquireCount: { r: 1 } } } protocol:op_command 0ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertEquals(
            parseJSON("""{"aggregate":"jstests_aggregation_server6290","pipeline":[{"${'$'}group":{"_id":0.0,"a":{"${'$'}first":{"${'$'}date":[{"year":1.0}]}}}}],"cursor":{"batchSize":0.0}}"""),
            (logEntries.first() as CommandLogEntry).command
        )
        assertNotNull((logEntries.first() as CommandLogEntry).exception)
        assertTrue((logEntries.first() as CommandLogEntry).code != 0)
    }

    @Test
    fun shouldSkipStatementDueToJsonShortening() {
        val logEntriesText = """2018-11-22T17:00:20.484+0100 I COMMAND  [conn620] command test.max_time_ms appName: "MongoDB Shell" command: find { find: "max_time_ms", filter: { ${'$'}where: function () {
if (this.slow) {
... }, batchSize: 3.0, sort: { _id: 1.0 }, maxTimeMS: 4000.0 } planSummary: IXSCAN { _id: 1 } cursorid:11501599070999 keysExamined:3 docsExamined:3 numYields:0 nreturned:3 reslen:152 locks:{ Global: { acquireCount: { r: 2 } }, Database: { acquireCount: { r: 1 } }, Collection: { acquireCount: { r: 1 } } } protocol:op_command 0ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertTrue(logEntries.first() is NoSupportedLogEntry)
    }

    @Test
    fun shoulCorreclyParseMissingCloseQuote() {
        val logEntriesText = """2018-11-22T17:01:04.118+0100 I COMMAND  [conn633] command test.mr_comments appName: "MongoDB Shell" command: mapReduce { mapreduce: "mr_comments", map: "// This will fail
function(){
    // Emit some stuff
    emit(this.foo, 1)
}
", reduce: function (key, values) {
        return Array.sum(values);
    }, out: "mr_comments_out" } planSummary: COUNT keysExamined:0 docsExamined:0 numYields:0 reslen:124 locks:{ Global: { acquireCount: { r: 42, w: 18, W: 2 } }, Database: { acquireCount: { r: 9, w: 9, R: 2, W: 11 } }, Collection: { acquireCount: { r: 9, w: 11 } }, Metadata: { acquireCount: { W: 1 } } } protocol:op_command 74ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(0, logEntries.size)
    }

    @Test
    fun shouldParseMapreduce() {
        val logEntriesText = """2018-11-22T17:01:00.349+0100 I COMMAND  [conn631] command test.mr_bigobject appName: "MongoDB Shell" command: mapReduce { mapreduce: "mr_bigobject", map: function () {
    emit(1, this.s);
}, reduce: function (k, v) {
    return 1;
}, out: "mr_bigobject_out" } planSummary: COUNT keysExamined:0 docsExamined:0 numYields:5 reslen:125 locks:{ Global: { acquireCount: { r: 48, w: 16, W: 2 } }, Database: { acquireCount: { r: 8, w: 7, R: 7, W: 12 } }, Collection: { acquireCount: { r: 8, w: 10 } }, Metadata: { acquireCount: { W: 1 } } } protocol:op_command 335ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertEquals(
            parseJSON("""
                {"mapreduce":"mr_bigobject","map":{"${'$'}code":"function () {\n emit(1, this.s);\n}"},"reduce":{"${'$'}code":"function (k, v) {\n return 1;\n}"},"out":"mr_bigobject_out"}
            """.trimIndent()),
            (logEntries.first() as CommandLogEntry).command
        )
    }

    @Test
    fun shouldManageComma() {
        val logEntriesText = """2018-11-22T17:01:30.999+0100 I WRITE    [conn700] remove test.jstests_or4 appName: "MongoDB Shell" query: { ${'$'}or: [ { a: 2.0 }, { b: 3.0 } ] } planSummary: IXSCAN { a: 1 }, IXSCAN { b: 1 } keysExamined:4 docsExamined:4 ndeleted:4 keysDeleted:12 numYields:0 locks:{ Global: { acquireCount: { r: 1, w: 1 } }, Database: { acquireCount: { w: 1 } }, Collection: { acquireCount: { w: 1 } } } 0ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        val entry = (logEntries.first() as WriteCommandLogEntry)
        assertEquals(
            parseJSON("""
                {"q":{"${'$'}or":[{"a":2.0},{"b":3.0}]}}
            """.trimIndent()),
            entry.command
        )
        assertEquals("IXSCAN", entry.planSummary.entries[0].type)
        assertEquals(JsonObject(mapOf(
            "a" to 1
        )), entry.planSummary.entries[0].document)
        assertEquals("IXSCAN", entry.planSummary.entries[1].type)
        assertEquals(JsonObject(mapOf(
            "b" to 1
        )), entry.planSummary.entries[1].document)
    }

    @Test
    fun shouldParseCorrectlyWithUnexpecteCharacter() {
        val logEntriesText = """2018-11-22T17:06:02.691+0100 I COMMAND  [conn1153] command test.roundtrip_basic appName: "MongoDB Shell" command: find { find: "roundtrip_basic", filter: { decimal: -Infinity }, sort: { decimal: 1.0, _id: 1.0 }, projection: { _id: 0.0 } } planSummary: COLLSCAN keysExamined:0 docsExamined:13 hasSortStage:1 cursorExhausted:1 numYields:0 nreturned:1 reslen:126 locks:{ Global: { acquireCount: { r: 2 } }, Database: { acquireCount: { r: 1 } }, Collection: { acquireCount: { r: 1 } } } protocol:op_command 0ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertEquals(
            parseJSON("""
                {"find":"roundtrip_basic","filter":{"decimal":"-Infinity"},"sort":{"decimal":1.0,"_id":1.0},"projection":{"_id":0.0}}
            """.trimIndent()),
            (logEntries.first() as CommandLogEntry).command
        )
    }

    @Test
    fun shouldManageToParseBinData() {
        val logEntriesText = """2018-11-22T17:05:14.227+0100 I COMMAND  [conn1059] command test.s6570 appName: "MongoDB Shell" command: aggregate { aggregate: "s6570", pipeline: [ { ${'$'}project: { str: { ${'$'}concat: [ BinData(0, ) ] } } } ], cursor: { batchSize: 0.0 } } exception: ${'$'}concat only supports strings, not binData code:16702 numYields:0 reslen:115 locks:{ Global: { acquireCount: { r: 4 } }, Database: { acquireCount: { r: 2 } }, Collection: { acquireCount: { r: 1 } } } protocol:op_command 0ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertEquals(
            parseJSON("""
                {"aggregate":"s6570","pipeline":[{"${'$'}project":{"str":{"${'$'}concat":[{"${'$'}binary":"","${'$'}type":"0"}]}}}],"cursor":{"batchSize":0.0}}
            """.trimIndent()),
            (logEntries.first() as CommandLogEntry).command
        )
    }

    @Test
    fun shouldParseWeirdTimestamp() {
        val logEntriesText = """2018-11-22T17:05:13.916+0100 I COMMAND  [conn1056] command test.jstests_aggregation_server6190 appName: "MongoDB Shell" command: aggregate { aggregate: "jstests_aggregation_server6190", pipeline: [ { ${'$'}project: { a: { ${'$'}week: Timestamp 441763200000|1000000000 } } }, { ${'$'}match: { a: { ${'$'}type: 16.0 } } } ], cursor: {} } planSummary: COLLSCAN keysExamined:0 docsExamined:1 cursorExhausted:1 numYields:0 nreturned:1 reslen:140 locks:{ Global: { acquireCount: { r: 8 } }, Database: { acquireCount: { r: 4 } }, Collection: { acquireCount: { r: 3 } } } protocol:op_command 0ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertEquals(
            parseJSON("""
                {"aggregate":"jstests_aggregation_server6190","pipeline":[{"${'$'}project":{"a":{"${'$'}week":{"${'$'}timestamp":{"t":4.417632E11,"i":1000000000}}}}},{"${'$'}match":{"a":{"${'$'}type":16.0}}}],"cursor":{}}
            """.trimIndent()),
            (logEntries.first() as CommandLogEntry).command
        )
    }

    @Test
    fun shouldParseComplex() {
        val logEntriesText = """2018-11-22T17:05:13.745+0100 I COMMAND  [conn1055] command test.c appName: "MongoDB Shell" command: aggregate { aggregate: "c", pipeline: [ { ${'$'}project: { _id: 0.0, year: { ${'$'}year: "${'$'}date" }, month: { ${'$'}month: "${'$'}date" }, dayOfMonth: { ${'$'}dayOfMonth: "${'$'}date" }, hour: { ${'$'}hour: "${'$'}date" }, minute: { ${'$'}minute: "${'$'}date" }, second: { ${'$'}second: "${'$'}date" }, millisecond: { ${'$'}millisecond: "${'$'}date" }, millisecondPlusTen: { ${'$'}millisecond: { ${'$'}add: [ "${'$'}date", 10.0 ] } }, string: { ${'$'}substr: [ "${'$'}date", 0.0, 1000.0 ] }, format: { ${'$'}dateToString: { format: "ISODate("%Y-%m-%dT%H:%M:%S.%LZ")", date: "${'$'}date" } } } } ] } planSummary: COLLSCAN keysExamined:0 docsExamined:1 numYields:0 nreturned:0 reslen:235 locks:{ Global: { acquireCount: { r: 8 } }, Database: { acquireCount: { r: 4 } }, Collection: { acquireCount: { r: 3 } } } protocol:op_command 0ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertEquals(
            parseJSON("""
                {"aggregate":"c","pipeline":[{"${'$'}project":{"_id":0.0,"year":{"${'$'}year":"${'$'}date"},"month":{"${'$'}month":"${'$'}date"},"dayOfMonth":{"${'$'}dayOfMonth":"${'$'}date"},"hour":{"${'$'}hour":"${'$'}date"},"minute":{"${'$'}minute":"${'$'}date"},"second":{"${'$'}second":"${'$'}date"},"millisecond":{"${'$'}millisecond":"${'$'}date"},"millisecondPlusTen":{"${'$'}millisecond":{"${'$'}add":["${'$'}date",10.0]}},"string":{"${'$'}substr":["${'$'}date",0.0,1000.0]},"format":{"${'$'}dateToString":{"format":"ISODate(\"%Y-%m-%dT%H:%M:%S.%LZ\")","date":"${'$'}date"}}}}]}
            """.trimIndent()),
            (logEntries.first() as CommandLogEntry).command
        )
    }

    @Test
    fun shouldParse1() {
        val logEntriesText = """2018-11-22T17:04:08.720+0100 I COMMAND  [conn987] command test.${'$'}cmd appName: "MongoDB Shell" command: update { update: "write_commands_reject_unknown_fields", updates: [ { q: {}, u: { ${'$'}inc: { a: 1.0 } } } ], asdf: true } exception: Unknown option to update command: asdf code:9 numYields:0 reslen:111 locks:{} protocol:op_command 0ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertEquals(
            parseJSON("""
                {"update":"write_commands_reject_unknown_fields","updates":[{"q":{},"u":{"${'$'}inc":{"a":1.0}}}],"asdf":true}
            """.trimIndent()),
            (logEntries.first() as CommandLogEntry).command
        )
        assertNotNull((logEntries.first() as CommandLogEntry).exception)
        assertNotEquals(0, (logEntries.first() as CommandLogEntry).code)
    }

    @Test
    fun shouldParse2() {
        val logEntriesText = """2018-11-22T17:03:47.825+0100 I WRITE    [conn933] update test.update_min_max appName: "MongoDB Shell" query: { _id: 6.0 } planSummary: IDHACK update: { ${'$'}min: { a: 1e-15.0 } } keysExamined:1 docsExamined:1 nMatched:1 nModified:1 numYields:0 locks:{ Global: { acquireCount: { r: 1, w: 1 } }, Database: { acquireCount: { w: 1 } }, Collection: { acquireCount: { w: 1 } } } 0ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertEquals(
            parseJSON("""
                {"q":{"_id":6.0},"u":{"${'$'}min":{"a":"1e-15.0"}}}
            """.trimIndent()),
            (logEntries.first() as WriteCommandLogEntry).command
        )
    }

    @Test
    fun shouldParseBinDataHex() {
        val logEntriesText = """2018-11-22T17:52:34.529+0100 I WRITE    [LogicalSessionCacheRefresh] update config.system.sessions command: { q: { _id: { id: UUID("71e7b7dc-8219-4be2-81a3-160f88b3dac0"), uid: BinData(0, E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855) } }, u: { ${'$'}currentDate: { lastUse: true } }, multi: false, upsert: true } planSummary: IDHACK keysExamined:0 docsExamined:0 nMatched:0 nModified:0 upsert:1 keysInserted:2 numYields:0 locks:{ Global: { acquireCount: { r: 1, w: 1 } }, Database: { acquireCount: { w: 1 } }, Collection: { acquireCount: { w: 1 } } } 0ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertEquals(
            parseJSON("""
                {"q":{"_id":{"id":{"${'$'}binary":"NzFlN2I3ZGMtODIxOS00YmUyLTgxYTMtMTYwZjg4YjNkYWMw","${'$'}type":"04"},"uid":{"${'$'}binary":"E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855","${'$'}type":"0"}}},"u":{"${'$'}currentDate":{"lastUse":true}},"multi":false,"upsert":true}
            """.trimIndent()),
            (logEntries.first() as WriteCommandLogEntry).command
        )
    }

    @Test
    fun shouldParse3() {
        val logEntriesText = """2018-11-22T17:37:15.637+0100 I COMMAND  [conn1203] command test.date_to_string appName: "MongoDB Shell" command: aggregate { aggregate: "date_to_string", pipeline: [ { ${'$'}project: { date: { ${'$'}dateToString: { format: "Natural: %Y-W%w-%U, ISO: %G-W%u-%V", date: "${'$'}date" } } } }, { ${'$'}sort: { _id: 1.0 } } ], cursor: {}, ${'$'}db: "test" } planSummary: COLLSCAN keysExamined:0 docsExamined:3 hasSortStage:1 cursorExhausted:1 numYields:0 nreturned:3 reslen:312 locks:{ Global: { acquireCount: { r: 2 } }, Database: { acquireCount: { r: 2 } }, Collection: { acquireCount: { r: 2 } } } protocol:op_command 0ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertEquals(
            parseJSON("""
                {"aggregate":"date_to_string","pipeline":[{"${'$'}project":{"date":{"${'$'}dateToString":{"format":"Natural: %Y-W%w-%U, ISO: %G-W%u-%V","date":"${'$'}date"}}}},{"${'$'}sort":{"_id":1.0}}],"cursor":{},"${'$'}db":"test"}
            """.trimIndent()),
            (logEntries.first() as CommandLogEntry).command
        )
    }

    @Test
    fun shouldParse4() {
        val logEntriesText = """2018-11-22T17:06:02.692+0100 I COMMAND  [conn1153] command test.roundtrip_basic appName: "MongoDB Shell" command: find { find: "roundtrip_basic", filter: { decimal: 9.999999999999999999999999999999999E+6144 }, sort: { decimal: 1.0, _id: 1.0 }, projection: { _id: 0.0 } } planSummary: COLLSCAN keysExamined:0 docsExamined:13 hasSortStage:1 cursorExhausted:1 numYields:0 nreturned:1 reslen:126 locks:{ Global: { acquireCount: { r: 2 } }, Database: { acquireCount: { r: 1 } }, Collection: { acquireCount: { r: 1 } } } protocol:op_command 0ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertEquals(
            parseJSON("""
                {"find":"roundtrip_basic","filter":{"decimal":{"${'$'}numberDecimal":"9.999999999999999999999999999999999E+6144"}},"sort":{"decimal":1.0,"_id":1.0},"projection":{"_id":0.0}}
            """.trimIndent()),
            (logEntries.first() as CommandLogEntry).command
        )
    }

    @Test
    fun shouldParse5() {
        val logEntriesText = """2018-11-22T17:02:57.879+0100 I WRITE    [conn825] update test.set6 appName: "MongoDB Shell" planSummary: COLLSCAN update: { ${'$'}set: { r.${'$'}id: 2.0 } } keysExamined:0 docsExamined:1 nMatched:1 nModified:1 numYields:0 locks:{ Global: { acquireCount: { r: 1, w: 1 } }, Database: { acquireCount: { w: 1 } }, Collection: { acquireCount: { w: 1 } } } 0ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertEquals(
            parseJSON("""
                {"u":{"${'$'}set":{"r.${'$'}id":2.0}}}
            """.trimIndent()),
            (logEntries.first() as WriteCommandLogEntry).command
        )
    }

    @Test
    fun shouldParse6() {
        val logEntriesText = """2018-11-22T17:05:27.622+0100 I COMMAND  [conn1111] command test.__unknown_name__ appName: "MongoDB Shell" command: insert { insert: "__unknown_name__", documents: [ { _id: 9.0, 2i: 18.0, 3i: 27.0 } ], ordered: true } ninserted:1 keysInserted:1 numYields:0 reslen:29 locks:{ Global: { acquireCount: { r: 1, w: 1 } }, Database: { acquireCount: { w: 1 } }, Collection: { acquireCount: { w: 1 } } } protocol:op_command 0ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertEquals(
            parseJSON("""
                {"insert":"__unknown_name__","documents":[{"_id":9.0,"2i":18.0,"3i":27.0}],"ordered":true}
            """.trimIndent()),
            (logEntries.first() as CommandLogEntry).command
        )
    }

    @Test
    fun shouldParse7() {
        val logEntriesText = """2018-11-22T17:04:02.149+0100 I COMMAND  [conn968] command views_aggregation.coll appName: "MongoDB Shell" command: aggregate { aggregate: "coll", pipeline: [ { ${'$'}out: "emptyPipelineView" } ] } planSummary: COLLSCAN exception: listIndexes failed: { ok: 0.0, errmsg: "Namespace views_aggregation.emptyPipelineView is a view, not a collection", code: 166, codeName: "CommandNotSupportedOnView" } code:18631 numYields:0 reslen:239 locks:{ Global: { acquireCount: { r: 10 } }, Database: { acquireCount: { r: 4, R: 1 } }, Collection: { acquireCount: { r: 3 } } } protocol:op_command 0ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertEquals(
            parseJSON("""
                {"aggregate":"coll","pipeline":[{"${'$'}out":"emptyPipelineView"}]}
            """.trimIndent()),
            (logEntries.first() as CommandLogEntry).command
        )
        assertNotNull((logEntries.first() as CommandLogEntry).exception)
        assertNotEquals(0, (logEntries.first() as CommandLogEntry).code)
    }

    @Test
    fun shouldParse8() {
        val logEntriesText = """2018-11-22T17:04:01.594+0100 I COMMAND  [conn965] command admin.system.users appName: "MongoDB Shell" command: insert { insert: "system.users", documents: [ { _id: "validate_user_documents.andy", user: "andy", db: "validate_user_documents", credentials: { SCRAM-SHA-1: { iterationCount: 10000, salt: "G151hc4zS/1jFRRvh7rS0A==", storedKey: "63wrdPCzCndehJASf2Gy3t4/O/s=", serverKey: "cd81a4P28zxzH/wT9G7Ih2zfKzg=" } }, roles: [ { role: "dbAdmin", db: "validate_user_documents" } ] } ] } ninserted:1 keysInserted:2 numYields:0 reslen:44 locks:{ Global: { acquireCount: { r: 3, w: 3 } }, Database: { acquireCount: { W: 3 } }, Collection: { acquireCount: { w: 3 } }, Metadata: { acquireCount: { W: 1 } } } protocol:op_query 0ms"""
        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
        val logEntries = mutableListOf<LogEntry>()

        logParser.forEach {
            logEntries += it
        }

        assertEquals(1, logEntries.size)
        assertEquals(
            parseJSON("""
                {"insert":"system.users","documents":[{"_id":"validate_user_documents.andy","user":"andy","db":"validate_user_documents","credentials":{"SCRAM-SHA-1":{"iterationCount":10000,"salt":"G151hc4zS/1jFRRvh7rS0A==","storedKey":"63wrdPCzCndehJASf2Gy3t4/O/s=","serverKey":"cd81a4P28zxzH/wT9G7Ih2zfKzg="}},"roles":[{"role":"dbAdmin","db":"validate_user_documents"}]}]}
            """.trimIndent()),
            (logEntries.first() as CommandLogEntry).command
        )
    }

//    @Test
//    fun shouldParse9() {
//        val logEntriesText = """2018-11-22T17:04:08.290+0100 I COMMAND  [conn983] command test.where2 appName: "MongoDB Shell" command: find { find: "where2", filter: { ${'$'}where: "
//this.a == 2" } } planSummary: COLLSCAN keysExamined:0 docsExamined:3 cursorExhausted:1 numYields:0 nreturned:1 reslen:120 locks:{ Global: { acquireCount: { r: 4 } }, Database: { acquireCount: { r: 2 } }, Collection: { acquireCount: { r: 2 } } } protocol:op_command 1ms"""
//        val logParser = LogParser(BufferedReader(StringReader(logEntriesText)), LogParserOptions(true))
//        val logEntries = mutableListOf<LogEntry>()
//
//        logParser.forEach {
//            logEntries += it
//        }
//
//        assertEquals(1, logEntries.size)
//        assertEquals(
//            parseJSON("""
//                {"insert":"system.users","documents":[{"_id":"validate_user_documents.andy","user":"andy","db":"validate_user_documents","credentials":{"SCRAM-SHA-1":{"iterationCount":10000,"salt":"G151hc4zS/1jFRRvh7rS0A==","storedKey":"63wrdPCzCndehJASf2Gy3t4/O/s=","serverKey":"cd81a4P28zxzH/wT9G7Ih2zfKzg="}},"roles":[{"role":"dbAdmin","db":"validate_user_documents"}]}]}
//            """.trimIndent()),
//            (logEntries.first() as CommandLogEntry).command
//        )
//    }

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