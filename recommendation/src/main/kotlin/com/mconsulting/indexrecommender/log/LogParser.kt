package com.mconsulting.indexrecommender.log

import jdk.nashorn.internal.runtime.regexp.RegExp
import org.bson.BsonDocument
import java.io.BufferedReader
import java.util.*

enum class SeverityLevels {
    F, E, W, I, D
}

enum class LogTypeNames {
    COMMAND, CONTROL, STORAGE, FTDC, NETWORK, RECOVERY
}

//2018-11-12T09:58:08.162+0100 I COMMAND  [conn1] command mindex_recommendation_tests.$cmd appName: "MongoDB Shell" command: profile { profile: 2.0, slowms: 0.0, lsid: { id: UUID("b8a51588-fc4d-4bfc-89e4-903ba7ffadc1") }, $db: "mindex_recommendation_tests" } numYields:0 reslen:79 locks:{ Global: { acquireCount: { r: 1, w: 1 } }, Database: { acquireCount: { W: 1 } } } protocol:op_msg 0ms
//2018-11-12T09:58:08.164+0100 I COMMAND  [conn1] command mindex_recommendation_tests.$cmd appName: "MongoDB Shell" command: isMaster { isMaster: 1.0, forShell: 1.0, $db: "mindex_recommendation_tests" } numYields:0 reslen:242 locks:{} protocol:op_msg 0ms
//2018-11-12T09:58:10.450+0100 I COMMAND  [conn1] command mindex_recommendation_tests.t appName: "MongoDB Shell" command: aggregate { aggregate: "t", pipeline: [ { $match: {} } ], cursor: {}, lsid: { id: UUID("b8a51588-fc4d-4bfc-89e4-903ba7ffadc1") }, $db: "mindex_recommendation_tests" } planSummary: COLLSCAN keysExamined:0 docsExamined:4 cursorExhausted:1 numYields:0 nreturned:4 reslen:403 locks:{ Global: { acquireCount: { r: 2 } }, Database: { acquireCount: { r: 2 } }, Collection: { acquireCount: { r: 2 } } } protocol:op_msg 0ms
//2018-11-12T09:58:10.452+0100 I COMMAND  [conn1] command mindex_recommendation_tests.$cmd appName: "MongoDB Shell" command: isMaster { isMaster: 1.0, forShell: 1.0, $db: "mindex_recommendation_tests" } numYields:0 reslen:242 locks:{} protocol:op_msg 0ms
//2018-11-12T09:58:58.960+0100 I COMMAND  [conn1] command mindex_recommendation_tests.t appName: "MongoDB Shell" command: find { find: "t", filter: { $text: { $search: "world" } }, limit: 1.0, singleBatch: true, lsid: { id: UUID("b8a51588-fc4d-4bfc-89e4-903ba7ffadc1") }, $db: "mindex_recommendation_tests" } planSummary: IXSCAN { _fts: "text", _ftsx: 1 } keysExamined:1 docsExamined:1 cursorExhausted:1 numYields:0 nreturned:1 queryHash:7E2D582B reslen:212 locks:{ Global: { acquireCount: { r: 2 } }, Database: { acquireCount: { r: 2 } }, Collection: { acquireCount: { r: 2 } } } protocol:op_msg 0ms
//2018-11-12T09:58:58.961+0100 I COMMAND  [conn1] command mindex_recommendation_tests.$cmd appName: "MongoDB Shell" command: isMaster { isMaster: 1.0, forShell: 1.0, $db: "mindex_recommendation_tests" } numYields:0 reslen:242 locks:{} protocol:op_msg 0ms

abstract class LogEntry

class PlanSummary(val type: String = "", val document: BsonDocument = BsonDocument())

class CommandLogEntry : LogEntry() {
    var appName: String = ""
    var command: BsonDocument = BsonDocument()
    var planSummary: PlanSummary = PlanSummary()
    var keysExamined: Int = -1
    var docsExamined: Int = -1
    var cursorExhausted: Int = 1
    var numYields: Int = -1
    var numberReturned: Int = -1
    var queryHash: String = ""
    var resultLength: Int = -1
    var locks: BsonDocument = BsonDocument()
    var protocol: String = ""
    var commandName: String = ""
    var executionTimeMS: Int = -1
}

class NoSupportedLogEntry : LogEntry()

class LogParser(val reader: BufferedReader) {
    private var line: String? = null

    fun hasNext() : Boolean {
        line = reader.readLine()

        if (line == null) {
            return false
        }

        return true
    }

    fun next() : LogEntry {
        if (line == null) {
            throw Exception("no line read")
        }

        // Current line
        val currentLine = line!!
        // Start parsing itK
        val tokenizer = StringTokenizer(currentLine, " ")
        // Get the date timestamp
        val dateTime = tokenizer.nextToken()
        // Read the log entry severity level
        val severityLevel = tokenizer.nextToken()
        // Read the log entry type
        val logType = tokenizer.nextToken()

        try {
            // Check that we have a valid line
            SeverityLevels.valueOf(severityLevel)
        } catch (ex: Exception) {
            throw Exception("not a legal MongoDB log line entry [$currentLine]")
        }

        // Process the types we know
        return when (logType.toUpperCase()) {
            LogTypeNames.COMMAND.name -> parseInfo(dateTime, severityLevel, currentLine, tokenizer)
            else -> NoSupportedLogEntry()
        }
    }

    private fun parseInfo(dateTime: String?, severityLevel: String?, line: String, tokenizer: StringTokenizer): LogEntry {
        // Read the connection information
        val connection = tokenizer.nextToken()
        // Skip the next token
        tokenizer.nextToken()
        // Read the namespace
        val namespace = tokenizer.nextToken()

        // We have a valid line
        if (line.contains("command: find")) {
            return parserFindCommand(line, tokenizer)
        }

        return NoSupportedLogEntry()
    }

    // 2018-11-12T09:58:58.960+0100
    // I
    // COMMAND
    // [conn1]
    // command mindex_recommendation_tests.t
    // appName: "MongoDB Shell"
    // command: find { find: "t", filter: { $text: { $search: "world" } }, limit: 1.0, singleBatch: true, lsid: { id: UUID("b8a51588-fc4d-4bfc-89e4-903ba7ffadc1") }, $db: "mindex_recommendation_tests" }
    // planSummary: IXSCAN { _fts: "text", _ftsx: 1 }
    // keysExamined:1
    // docsExamined:1
    // cursorExhausted:1
    // numYields:0
    // nreturned:1
    // queryHash:7E2D582B
    // reslen:212
    // locks:{ Global: { acquireCount: { r: 2 } }, Database: { acquireCount: { r: 2 } }, Collection: { acquireCount: { r: 2 } } }
    // protocol:op_msg
    // 0ms
    private fun parserFindCommand(line: String, tokenizer: StringTokenizer) : LogEntry {
        // Split the line into ever key we can
        var restOfLine = tokenizer.toList().joinToString(" ")

        // Create command log entry
        val entry = CommandLogEntry()

        // Extract the miliseconds execution
        val msMatch = Regex("""(\d)+ms""").find(restOfLine)
        // Get match and grab the number of milliseconds
        if (msMatch != null) {
            entry.executionTimeMS = msMatch.groups.last()!!.value.toInt()
            // Replace the milliseconds string
            restOfLine = restOfLine.replace(Regex("""(\d)+ms"""), "")
        }

        // Get the parts
        var parts = mutableListOf(restOfLine)

        // Keys to split by
        val keys = listOf(
            "appName:", "command:", "planSummary:",
            "keysExamined:", "docsExamined:", "cursorExhausted:",
            "numYields:", "nreturned:", "queryHash:",
            "reslen:", "locks:", "protocol:")

        for (key in keys) {
            var currentParts = mutableListOf<String>()

            for (p in parts) {
                if (p.contains(key)) {
                    val leftSide = p.substringBefore(key)
                    val rightSide = "$key${p.substringAfter(key)}"
                    currentParts.addAll(listOf(leftSide, rightSide))
                } else {
                    currentParts.add(p)
                }
            }

            parts = currentParts
        }

        // Process all the entries
        parts.forEach {
            if (it.contains("appName:")) {
                entry.appName = it
                    .split("appName:")
                    .last()
                    .trim()
                    .replace("\"", "")
            } else if (it.contains("keysExamined:")) {
               entry.keysExamined = it
                   .split("keysExamined:").last().trim().toInt()
            } else if (it.contains("docsExamined:")) {
                entry.docsExamined = it
                    .split("docsExamined:").last().trim().toInt()
            } else if (it.contains("numYields:")) {
                entry.numYields = it
                    .split("numYields:").last().trim().toInt()
            } else if (it.contains("nreturned:")) {
                entry.numberReturned = it
                    .split("nreturned:").last().trim().toInt()
            } else if (it.contains("reslen:")) {
                entry.resultLength = it
                    .split("reslen:").last().trim().toInt()
            } else if (it.contains("cursorExhausted:")) {
                entry.cursorExhausted = it
                    .split("cursorExhausted:").last().trim().toInt()
            } else if (it.contains("queryHash:")) {
                entry.queryHash = it
                    .split("queryHash:").last().trim()
            } else if (it.contains("protocol:")) {
                entry.protocol = it
                    .split("protocol:").last().trim()
            } else if (it.contains("locks:")) {
                entry.locks = commandToBsonDocument(it
                    .split("locks:").last().trim())
            } else if (it.contains("command:")) {
                val token = StringTokenizer(it)
                token.nextToken()
                entry.commandName = token.nextToken().trim()
                entry.command = commandToBsonDocument(token.toList().joinToString(" ").trim())
            } else if (it.contains("planSummary:")) {
                val token = StringTokenizer(it)
                token.nextToken()
                val scanType = token.nextToken().trim()
                val scanDocument = commandToBsonDocument(token.toList().joinToString(" "))
                entry.planSummary = PlanSummary(scanType, scanDocument)
            }
        }

        return entry
    }

    /**
    {
        find: "t",
        filter: {
            $text: {
                $search: "world"
            }
        },
        limit: 1.0,
        singleBatch: true,
        lsid: {
            id: UUID("b8a51588-fc4d-4bfc-89e4-903ba7ffadc1")
        },
        $db: "mindex_recommendation_tests"
    }
     */
    private fun commandToBsonDocument(json: String): BsonDocument {
        // Rewrite all the keys to be json compatible
        var modifiedJson = json.replace(Regex("""([\d|\w|\$|\.|\_]+)\:"""), "\"$1\":")
        // Rewrite any UUID field as a bson type
        modifiedJson = modifiedJson.replace(
            Regex("""UUID\("([\d|\w|\-]+)"\)"""),
            "{\"\\${'$'}binary\": \"$1\", \"\\${'$'}type\": \"4\"}")
        return BsonDocument.parse(modifiedJson)
    }

    private fun readJson(tokenizer: StringTokenizer): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    fun forEach(function: (entry: LogEntry) -> Unit) {
        while (hasNext()) {
            function(next())
        }
    }
}