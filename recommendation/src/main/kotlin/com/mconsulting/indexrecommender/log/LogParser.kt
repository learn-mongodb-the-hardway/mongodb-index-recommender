package com.mconsulting.indexrecommender.log

import java.io.BufferedReader
import java.io.StringReader
import java.util.*

enum class SeverityLevels {
    F, E, W, I, D
}

//2018-11-12T09:58:08.162+0100 I COMMAND  [conn1] command mindex_recommendation_tests.$cmd appName: "MongoDB Shell" command: profile { profile: 2.0, slowms: 0.0, lsid: { id: UUID("b8a51588-fc4d-4bfc-89e4-903ba7ffadc1") }, $db: "mindex_recommendation_tests" } numYields:0 reslen:79 locks:{ Global: { acquireCount: { r: 1, w: 1 } }, Database: { acquireCount: { W: 1 } } } protocol:op_msg 0ms
//2018-11-12T09:58:08.164+0100 I COMMAND  [conn1] command mindex_recommendation_tests.$cmd appName: "MongoDB Shell" command: isMaster { isMaster: 1.0, forShell: 1.0, $db: "mindex_recommendation_tests" } numYields:0 reslen:242 locks:{} protocol:op_msg 0ms
//2018-11-12T09:58:10.450+0100 I COMMAND  [conn1] command mindex_recommendation_tests.t appName: "MongoDB Shell" command: aggregate { aggregate: "t", pipeline: [ { $match: {} } ], cursor: {}, lsid: { id: UUID("b8a51588-fc4d-4bfc-89e4-903ba7ffadc1") }, $db: "mindex_recommendation_tests" } planSummary: COLLSCAN keysExamined:0 docsExamined:4 cursorExhausted:1 numYields:0 nreturned:4 reslen:403 locks:{ Global: { acquireCount: { r: 2 } }, Database: { acquireCount: { r: 2 } }, Collection: { acquireCount: { r: 2 } } } protocol:op_msg 0ms
//2018-11-12T09:58:10.452+0100 I COMMAND  [conn1] command mindex_recommendation_tests.$cmd appName: "MongoDB Shell" command: isMaster { isMaster: 1.0, forShell: 1.0, $db: "mindex_recommendation_tests" } numYields:0 reslen:242 locks:{} protocol:op_msg 0ms
//2018-11-12T09:58:58.960+0100 I COMMAND  [conn1] command mindex_recommendation_tests.t appName: "MongoDB Shell" command: find { find: "t", filter: { $text: { $search: "world" } }, limit: 1.0, singleBatch: true, lsid: { id: UUID("b8a51588-fc4d-4bfc-89e4-903ba7ffadc1") }, $db: "mindex_recommendation_tests" } planSummary: IXSCAN { _fts: "text", _ftsx: 1 } keysExamined:1 docsExamined:1 cursorExhausted:1 numYields:0 nreturned:1 queryHash:7E2D582B reslen:212 locks:{ Global: { acquireCount: { r: 2 } }, Database: { acquireCount: { r: 2 } }, Collection: { acquireCount: { r: 2 } } } protocol:op_msg 0ms
//2018-11-12T09:58:58.961+0100 I COMMAND  [conn1] command mindex_recommendation_tests.$cmd appName: "MongoDB Shell" command: isMaster { isMaster: 1.0, forShell: 1.0, $db: "mindex_recommendation_tests" } numYields:0 reslen:242 locks:{} protocol:op_msg 0ms
class LogParser(val reader: BufferedReader) {
    private var line: String? = null

    fun hasNext() : Boolean {
        line = reader.readLine()

        if (line == null) {
            return false
        }

        return true
    }

    fun next() {
        if (line == null) throw Exception("no line read")
        // Start parsing itK
        val tokenizer = StringTokenizer(line, " ")
        // Get the date timestamp
        val dateTime = tokenizer.nextToken()
        val severityLevel = tokenizer.nextToken()
        val logType = tokenizer.nextToken()
        val connection = tokenizer.nextToken()

        try {
            // Check that we have a valid line
            SeverityLevels.valueOf(severityLevel)

            // We have a valid line


        } catch (ex: Exception) {
            println("ERROR")
        }
    }
}