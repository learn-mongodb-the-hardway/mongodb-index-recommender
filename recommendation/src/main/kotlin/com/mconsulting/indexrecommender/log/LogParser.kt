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
    fun update(values: MutableMap<String, Any>): CommandLogEntry {
        if (values.containsKey("appName")) appName = values.get("appName") as String
        if (values.containsKey("command")) command = values.get("command") as BsonDocument
        if (values.containsKey("planSummary")) planSummary = values.get("planSummary") as PlanSummary
        if (values.containsKey("keysExamined")) keysExamined = values.get("keysExamined") as Int
        if (values.containsKey("docsExamined")) docsExamined = values.get("docsExamined") as Int
        if (values.containsKey("cursorExhausted")) cursorExhausted = values.get("cursorExhausted") as Int
        if (values.containsKey("numYields")) numYields = values.get("numYields") as Int
        if (values.containsKey("numberReturned")) numberReturned = values.get("numberReturned") as Int
        if (values.containsKey("queryHash")) queryHash = values.get("queryHash") as String
        if (values.containsKey("resultLength")) resultLength = values.get("resultLength") as Int
        if (values.containsKey("locks")) locks = values.get("locks") as BsonDocument
        if (values.containsKey("protocol")) protocol = values.get("protocol") as String
        if (values.containsKey("commandName")) commandName = values.get("commandName") as String
        if (values.containsKey("executionTimeMS")) executionTimeMS = values.get("executionTimeMS") as Int
        return this
    }

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

class WriteCommandLogEntry(var commandName: String, dateTime: DateTime, severityLevel: SeverityLevels, namespace: Namespace) : LogEntryBase(
    dateTime, severityLevel, namespace
) {

    fun update(values: MutableMap<String, Any>): WriteCommandLogEntry {
        if (values.containsKey("appName")) appName = values.get("appName") as String
        if (values.containsKey("command")) command = values.get("command") as BsonDocument
        if (values.containsKey("planSummary")) planSummary = values.get("planSummary") as PlanSummary
        if (values.containsKey("keysExamined")) keysExamined = values.get("keysExamined") as Int
        if (values.containsKey("docsExamined")) docsExamined = values.get("docsExamined") as Int
        if (values.containsKey("cursorExhausted")) cursorExhausted = values.get("cursorExhausted") as Int
        if (values.containsKey("numYields")) numYields = values.get("numYields") as Int
        if (values.containsKey("numberReturned")) numberReturned = values.get("numberReturned") as Int
        if (values.containsKey("queryHash")) queryHash = values.get("queryHash") as String
        if (values.containsKey("resultLength")) resultLength = values.get("resultLength") as Int
        if (values.containsKey("locks")) locks = values.get("locks") as BsonDocument
        if (values.containsKey("protocol")) protocol = values.get("protocol") as String
        if (values.containsKey("commandName")) commandName = values.get("commandName") as String
        if (values.containsKey("executionTimeMS")) executionTimeMS = values.get("executionTimeMS") as Int
        return this
    }

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

        // Create string tokenizer
        val partsTokenizer = StringTokenizer(restOfLine)
        // Keep parsing until end
        val values = extractLogLineParts(partsTokenizer)
        // Update the value
        return entry.update(values)
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

        // Create string tokenizer
        val partsTokenizer = StringTokenizer(restOfLine)
        // Keep parsing until end
        val values = extractLogLineParts(partsTokenizer)
        // Update the value
        return entry.update(values)
    }

    private fun extractLogLineParts(partsTokenizer: StringTokenizer) : MutableMap<String, Any> {
        val values = mutableMapOf<String, Any>(
            "command" to BsonDocument()
        )

        while (partsTokenizer.hasMoreTokens()) {
            val token = partsTokenizer.nextToken()

            if (token.startsWith("command:")) {
                val token = partsTokenizer.nextToken()
                val json: String

                if (token == "{") {
                    json = readJson(partsTokenizer, token)
                } else {
                    values["commandName"] = token
                    json = readJson(partsTokenizer)
                }

                values["command"] = commandToBsonDocument(json)
            } else if (token.startsWith("appName:")) {
                val tokens = mutableListOf<String>()

                while (true) {
                    val value = partsTokenizer.nextToken()

                    if (value.last() == '"') {
                        tokens += value.substring(0, value.length - 1)
                        break
                    }

                    if (tokens.size == 0) {
                        tokens += value.substring(1)
                    } else {
                        tokens += value
                    }
                }

                values["appName"] = tokens.joinToString(" ")
            } else if (token.startsWith("keysExamined:")) {
                values["keysExamined"] = extractInt(token, partsTokenizer)
            } else if (token.startsWith("docsExamined:")) {
                values["docsExamined"] = extractInt(token, partsTokenizer)
            } else if (token.startsWith("numYields:")) {
                values["numYields"] = extractInt(token, partsTokenizer)
            } else if (token.startsWith("nreturned:")) {
                values["nreturned"] = extractInt(token, partsTokenizer)
            } else if (token.startsWith("nMatched:")) {
                values["nMatched"] = extractInt(token, partsTokenizer)
            } else if (token.startsWith("nModified:")) {
                values["nModified"] = extractInt(token, partsTokenizer)
            } else if (token.startsWith("ndeleted:")) {
                values["ndeleted"] = extractInt(token, partsTokenizer)
            } else if (token.startsWith("keysDeleted:")) {
                values["keysDeleted"] = extractInt(token, partsTokenizer)
            } else if (token.startsWith("reslen:")) {
                values["reslen"] = extractInt(token, partsTokenizer)
            } else if (token.startsWith("cursorExhausted:")) {
                values["cursorExhausted"] = extractInt(token, partsTokenizer)
            } else if (token.startsWith("queryHash:")) {
                values["queryHash"] = extractString(token, partsTokenizer)
            } else if (token.startsWith("protocol:")) {
                values["protocol"] = extractString(token, partsTokenizer)
            } else if (token.startsWith("locks:")) {
                if (token.contains("{") && token.contains("}")) {
                    values["locks"] = BsonDocument()
                } else if (token.contains("{")) {
                    values["locks"] = commandToBsonDocument(readJson(partsTokenizer, "{"))
                } else {
                    values["locks"] = commandToBsonDocument(readJson(partsTokenizer))
                }
            } else if (token.startsWith("update:")) {
                val update = commandToBsonDocument(readJson(partsTokenizer))

                if (values.containsKey("command")) {
                    (values.get("command") as BsonDocument).append("u", update)
                }

                println()
            } else if (token.startsWith("query:")) {
                val query = commandToBsonDocument(readJson(partsTokenizer))

                if (values.containsKey("command")) {
                    (values.get("command") as BsonDocument).append("q", query)
                }
                println()
            } else if (token.startsWith("planSummary:")) {
                val scanType = partsTokenizer.nextToken().trim()
                var document = BsonDocument()

                if (scanType.toUpperCase() == "IXSCAN") {
                    document = commandToBsonDocument(readJson(partsTokenizer))
                }

                values["planSummary"] = PlanSummary(scanType, document)
            }
        }

        return values
    }

    private fun readJson(partsTokenizer: StringTokenizer, token: String? = null): String {
        var depth = 0
        val tokens = mutableListOf<String>()

        if (token != null) {
            depth = 1
            tokens += token
        }

        do {
            val nextToken = partsTokenizer.nextToken()

            if (nextToken.startsWith("{") && nextToken.contains("}")) {
            } else if (nextToken.startsWith("{")) {
                depth += 1
            } else if (nextToken.startsWith("}")) {
                depth -= 1
            }

            tokens += nextToken
        } while (depth != 0 && partsTokenizer.hasMoreTokens())

        if (depth != 0) {
            throw Exception("illegal json string [${tokens.joinToString(" ")}]")
        }

        return tokens.joinToString(" ")
    }

    fun forEach(function: (entry: LogEntry) -> Unit) {
        while (hasNext()) {
            function(next())
        }
    }
}

fun extractInt(token: String, tokenizer: StringTokenizer) : Int = extractString(token, tokenizer).toInt()

fun extractString(token: String, tokenizer: StringTokenizer) : String {
    val parts = token.split(":")
    var value = parts[1]

    if (value == "") {
        value = tokenizer.nextToken()
    }

    return value
}
