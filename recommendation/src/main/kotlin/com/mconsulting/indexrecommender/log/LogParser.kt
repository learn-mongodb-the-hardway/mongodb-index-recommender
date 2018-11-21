package com.mconsulting.indexrecommender.log

import com.mconsulting.indexrecommender.Namespace
import com.mconsulting.indexrecommender.commandToBsonDocument
import org.bson.BsonDocument
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import java.io.BufferedReader
import java.util.*

enum class SeverityLevels {
    F, E, W, I, D
}

enum class LogTypeNames {
    COMMAND, CONTROL, STORAGE, FTDC, NETWORK, RECOVERY, WRITE
}

//2018-11-12T09:58:08.162+0100 I COMMAND  [conn1] command mindex_recommendation_tests.$cmd appName: "MongoDB Shell" command: profile { profile: 2.0, slowms: 0.0, lsid: { id: UUID("b8a51588-fc4d-4bfc-89e4-903ba7ffadc1") }, $db: "mindex_recommendation_tests" } numYields:0 reslen:79 locks:{ Global: { acquireCount: { r: 1, w: 1 } }, Database: { acquireCount: { W: 1 } } } protocol:op_msg 0ms
//2018-11-12T09:58:08.164+0100 I COMMAND  [conn1] command mindex_recommendation_tests.$cmd appName: "MongoDB Shell" command: isMaster { isMaster: 1.0, forShell: 1.0, $db: "mindex_recommendation_tests" } numYields:0 reslen:242 locks:{} protocol:op_msg 0ms
//2018-11-12T09:58:10.450+0100 I COMMAND  [conn1] command mindex_recommendation_tests.t appName: "MongoDB Shell" command: aggregate { aggregate: "t", pipeline: [ { $match: {} } ], cursor: {}, lsid: { id: UUID("b8a51588-fc4d-4bfc-89e4-903ba7ffadc1") }, $db: "mindex_recommendation_tests" } planSummary: COLLSCAN keysExamined:0 docsExamined:4 cursorExhausted:1 numYields:0 nreturned:4 reslen:403 locks:{ Global: { acquireCount: { r: 2 } }, Database: { acquireCount: { r: 2 } }, Collection: { acquireCount: { r: 2 } } } protocol:op_msg 0ms
//2018-11-12T09:58:10.452+0100 I COMMAND  [conn1] command mindex_recommendation_tests.$cmd appName: "MongoDB Shell" command: isMaster { isMaster: 1.0, forShell: 1.0, $db: "mindex_recommendation_tests" } numYields:0 reslen:242 locks:{} protocol:op_msg 0ms
//2018-11-12T09:58:58.960+0100 I COMMAND  [conn1] command mindex_recommendation_tests.t appName: "MongoDB Shell" command: find { find: "t", filter: { $text: { $search: "world" } }, limit: 1.0, singleBatch: true, lsid: { id: UUID("b8a51588-fc4d-4bfc-89e4-903ba7ffadc1") }, $db: "mindex_recommendation_tests" } planSummary: IXSCAN { _fts: "text", _ftsx: 1 } keysExamined:1 docsExamined:1 cursorExhausted:1 numYields:0 nreturned:1 queryHash:7E2D582B reslen:212 locks:{ Global: { acquireCount: { r: 2 } }, Database: { acquireCount: { r: 2 } }, Collection: { acquireCount: { r: 2 } } } protocol:op_msg 0ms
//2018-11-12T09:58:58.961+0100 I COMMAND  [conn1] command mindex_recommendation_tests.$cmd appName: "MongoDB Shell" command: isMaster { isMaster: 1.0, forShell: 1.0, $db: "mindex_recommendation_tests" } numYields:0 reslen:242 locks:{} protocol:op_msg 0ms

interface LogEntry

abstract class LogEntryBase(
    val timestamp: DateTime,
    val severityLevel: SeverityLevels,
    val namespace: Namespace) : LogEntry

class PlanSummary(val type: String = "", val document: BsonDocument = BsonDocument())

class CommandLogEntry(dateTime: DateTime, severityLevel: SeverityLevels, namespace: Namespace) : LogEntryBase(
    dateTime, severityLevel, namespace
) {
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

class WriteCommandLogEntry(val commandName: String, dateTime: DateTime, severityLevel: SeverityLevels, namespace: Namespace) : LogEntryBase(
    dateTime, severityLevel, namespace
) {
    var appName: String = ""
    var command: BsonDocument = BsonDocument()
    var planSummary: PlanSummary = PlanSummary()
    var keysExamined: Int = -1
    var docsExamined: Int = -1
    var cursorExhausted: Int = 1
    var numYields: Int = -1
    var numberReturned: Int = -1
    var nModified: Int = -1
    var nMatched: Int = -1
    var keysDeleted: Int = -1
    var ndeleted: Int = -1
    var queryHash: String = ""
    var resultLength: Int = -1
    var locks: BsonDocument = BsonDocument()
    var protocol: String = ""
    var executionTimeMS: Int = -1
}

class NoSupportedLogEntry(val line: String) : LogEntry

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
        val dateTimeString = tokenizer.nextToken()
        // 2018-11-12T09:57:00.792+0100
        val formatter = ISODateTimeFormat.dateTime()
        val dateTime = formatter.parseDateTime(dateTimeString)
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
            LogTypeNames.COMMAND.name -> parseInfo(dateTime, SeverityLevels.valueOf(severityLevel), tokenizer)
            LogTypeNames.WRITE.name -> parseWrite(dateTime, SeverityLevels.valueOf(severityLevel), tokenizer)
            else -> NoSupportedLogEntry(currentLine)
        }
    }

    private fun parseWrite(dateTime: DateTime, severityLevel: SeverityLevels, tokenizer: StringTokenizer): LogEntry {
        // Read the connection information
        val connection = tokenizer.nextToken()
        // Skip the next token
        val commandName = tokenizer.nextToken().trim()
        // Read the namespace
        val namespaceString = tokenizer.nextToken()
        val namespace = Namespace.parse(namespaceString)

        // Split the line into ever key we can
        var restOfLine = tokenizer.toList().joinToString(" ")

        // Create command log entry
        val entry = WriteCommandLogEntry(commandName, dateTime, severityLevel, namespace)

        // Extract the miliseconds execution
        val msMatch = Regex("""(\d)+ms""").find(restOfLine)
        // Get match and grab the number of milliseconds
        if (msMatch != null) {
            entry.executionTimeMS = msMatch.groups.last()!!.value.toInt()
            // Replace the milliseconds string
            restOfLine = restOfLine.replace(Regex("""(\d)+ms"""), "")
        }

        val parts: List<String>

        if (!restOfLine.contains(Regex("command: [\\s]+ "))) {
            parts = splitLogLine(mutableListOf(restOfLine), listOf("query:", "update:"))
        } else {
            parts = splitLogLine(mutableListOf(restOfLine))
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
            } else if (it.contains("nModified:")) {
                entry.nModified = it
                    .split("nModified:").last().trim().toInt()
            } else if (it.contains("nMatched:")) {
                entry.nMatched = it
                    .split("nMatched:").last().trim().toInt()
            } else if (it.contains("ndeleted:")) {
                entry.ndeleted = it
                    .split("ndeleted:").last().trim().toInt()
            } else if (it.contains("keysDeleted:")) {
                entry.keysDeleted = it
                    .split("keysDeleted:").last().trim().toInt()
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
                entry.command = commandToBsonDocument(token.toList().joinToString(" ").trim())
            } else if (it.contains("query:")) {
                val token = StringTokenizer(it)
                token.nextToken()
                val query = commandToBsonDocument(token.toList().joinToString(" ").trim())
                entry.command.append("q", query)
            } else if (it.contains("update:")) {
                val token = StringTokenizer(it)
                token.nextToken()
                val update = commandToBsonDocument(token.toList().joinToString(" ").trim())
                entry.command.append("u", update)
            } else if (it.contains("planSummary:")) {
                val token = StringTokenizer(it)
                token.nextToken()
                val scanType = token.nextToken().trim()
                val restOfSummary = token.toList().joinToString(" ")
                var scanDocument = BsonDocument()

                if (restOfSummary.contains("{")) {
                    scanDocument = commandToBsonDocument(restOfSummary)
                }

                entry.planSummary = PlanSummary(scanType, scanDocument)
            }
        }

        return entry
    }

    private fun splitLogLine(parts: MutableList<String>, additionalSplits: List<String> = listOf()): MutableList<String> {
        // Keys to split by
        var parts = parts
        val keys = listOf(
            "appName:", "command:", "planSummary:",
            "keysExamined:", "docsExamined:", "cursorExhausted:",
            "numYields:", "nreturned:", "queryHash:",
            "reslen:", "locks:", "protocol:", "nMatched:",
            "nModified:", "ndeleted:", "keysDeleted:") + additionalSplits

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
        return parts
    }

    private fun parseInfo(dateTime: DateTime, severityLevel: SeverityLevels, tokenizer: StringTokenizer): LogEntry {
        // Read the connection information
        tokenizer.nextToken()
        // Skip the next token
        tokenizer.nextToken()
        // Read the namespace
        val namespaceString = tokenizer.nextToken()
        val namespace = Namespace.parse(namespaceString)
        // We have a valid line
        return parseCommand(tokenizer, dateTime, severityLevel, namespace)
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
    private fun parseCommand(tokenizer: StringTokenizer, dateTime: DateTime, severityLevel: SeverityLevels, namespace: Namespace) : LogEntry {
        // Split the line into ever key we can
        var restOfLine = tokenizer.toList().joinToString(" ")

        // Create command log entry
        val entry = CommandLogEntry(dateTime, severityLevel, namespace)

        // Extract the miliseconds execution
        val msMatch = Regex("""(\d)+ms""").find(restOfLine)
        // Get match and grab the number of milliseconds
        if (msMatch != null) {
            entry.executionTimeMS = msMatch.groups.last()!!.value.toInt()
            // Replace the milliseconds string
            restOfLine = restOfLine.replace(Regex("""(\d)+ms"""), "")
        }

        // Get the parts
        val parts = splitLogLine(mutableListOf(restOfLine))

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
                val restOfSummary = token.toList().joinToString(" ")
                var scanDocument = BsonDocument()

                if (restOfSummary.contains("{")) {
                    scanDocument = commandToBsonDocument(restOfSummary)
                }

                entry.planSummary = PlanSummary(scanType, scanDocument)
            }
        }

        return entry
    }

    fun forEach(function: (entry: LogEntry) -> Unit) {
        while (hasNext()) {
            function(next())
        }
    }
}