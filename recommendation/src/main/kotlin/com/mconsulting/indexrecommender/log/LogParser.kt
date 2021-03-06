package com.mconsulting.indexrecommender.log

import com.beust.klaxon.JsonObject
import com.mconsulting.indexrecommender.Namespace
import com.mconsulting.indexrecommender.commandToJsonObject
import com.mconsulting.indexrecommender.readDoubleQuotedString
import mu.KLogging
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

val tags = listOf(
    "code", "command", "appName", "keysExamined", "docsExamined",
    "numYields", "nreturned", "nMatched", "nModified", "ndeleted",
    "keysDeleted", "ninserted", "keysInserted", "reslen", "cursorExhausted",
    "queryHash", "protocol", "locks", "update", "query", "planSummary", "exception"
)

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
    val namespace: Namespace, var exception: String? = null, var code: Int = 0) : LogEntry

class PlanSummaryEntry(val type: String = "", val document: JsonObject = JsonObject())

class PlanSummary(val entries: List<PlanSummaryEntry> = listOf())

val VALIDATE_SHORTENED = Regex("[ ]*[\\.]{3}[ |,]*")

class CommandLogEntry(dateTime: DateTime, severityLevel: SeverityLevels, namespace: Namespace) : LogEntryBase(
    dateTime, severityLevel, namespace
) {
    fun update(values: MutableMap<String, Any>): CommandLogEntry {
        if (values.containsKey("appName")) appName = values.get("appName") as String
        if (values.containsKey("command")) command = values.get("command") as JsonObject
        if (values.containsKey("planSummary")) planSummary = values.get("planSummary") as PlanSummary
        if (values.containsKey("keysExamined")) keysExamined = values.get("keysExamined") as Int
        if (values.containsKey("docsExamined")) docsExamined = values.get("docsExamined") as Int
        if (values.containsKey("cursorExhausted")) cursorExhausted = values.get("cursorExhausted") as Int
        if (values.containsKey("numYields")) numYields = values.get("numYields") as Int
        if (values.containsKey("numberReturned")) numberReturned = values.get("numberReturned") as Int
        if (values.containsKey("queryHash")) queryHash = values.get("queryHash") as String
        if (values.containsKey("resultLength")) resultLength = values.get("resultLength") as Int
        if (values.containsKey("locks")) locks = values.get("locks") as JsonObject
        if (values.containsKey("protocol")) protocol = values.get("protocol") as String
        if (values.containsKey("commandName")) commandName = values.get("commandName") as String
        if (values.containsKey("executionTimeMS")) executionTimeMS = values.get("executionTimeMS") as Int
        if (values.containsKey("exception")) exception = values["exception"] as String
        if (values.containsKey("code")) code = values["code"] as Int
        if (values.containsKey("errMsg")) errMsg = values["errMsg"] as String
        if (values.containsKey("errName")) errName = values["errName"] as String
        if (values.containsKey("errCode")) errCode = values["errCode"] as Int
        return this
    }

    var appName: String = ""
    var command: JsonObject = JsonObject()
    var planSummary: PlanSummary = PlanSummary()
    var keysExamined: Int = -1
    var docsExamined: Int = -1
    var cursorExhausted: Int = 1
    var numYields: Int = -1
    var numberReturned: Int = -1
    var queryHash: String = ""
    var resultLength: Int = -1
    var locks: JsonObject = JsonObject()
    var protocol: String = ""
    var commandName: String = ""
    var executionTimeMS: Int = -1
    var errMsg: String = ""
    var errName: String = ""
    var errCode: Int = 0
}

class WriteCommandLogEntry(var commandName: String, dateTime: DateTime, severityLevel: SeverityLevels, namespace: Namespace) : LogEntryBase(
    dateTime, severityLevel, namespace
) {

    fun update(values: MutableMap<String, Any>): WriteCommandLogEntry {
        if (values.containsKey("appName")) appName = values["appName"] as String
        if (values.containsKey("command")) command = values["command"] as JsonObject
        if (values.containsKey("planSummary")) planSummary = values["planSummary"] as PlanSummary
        if (values.containsKey("keysExamined")) keysExamined = values["keysExamined"] as Int
        if (values.containsKey("docsExamined")) docsExamined = values["docsExamined"] as Int
        if (values.containsKey("cursorExhausted")) cursorExhausted = values["cursorExhausted"] as Int
        if (values.containsKey("numYields")) numYields = values["numYields"] as Int
        if (values.containsKey("numberReturned")) numberReturned = values["numberReturned"] as Int
        if (values.containsKey("queryHash")) queryHash = values["queryHash"] as String
        if (values.containsKey("resultLength")) resultLength = values["resultLength"] as Int
        if (values.containsKey("locks")) locks = values["locks"] as JsonObject
        if (values.containsKey("protocol")) protocol = values["protocol"] as String
        if (values.containsKey("commandName")) commandName = values["commandName"] as String
        if (values.containsKey("executionTimeMS")) executionTimeMS = values["executionTimeMS"] as Int
        if (values.containsKey("exception")) exception = values["exception"] as String
        if (values.containsKey("code")) code = values["code"] as Int
        if (values.containsKey("errMsg")) errMsg = values["errMsg"] as String
        if (values.containsKey("errName")) errName = values["errName"] as String
        if (values.containsKey("errCode")) errCode = values["errCode"] as Int
        return this
    }

    var appName: String = ""
    var command: JsonObject = JsonObject()
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
    var locks: JsonObject = JsonObject()
    var protocol: String = ""
    var executionTimeMS: Int = -1
    var errMsg: String = ""
    var errName: String = ""
    var errCode: Int = 0
}

class NoSupportedLogEntry(val line: String) : LogEntry

// 2018-11-22T17:00:20.456+0100
val dateMatch = Regex("^[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}T[0-9]{2}\\:[0-9]{2}\\:[0-9]{2}\\.[0-9]{3}\\+[0-9]{4}")

class LogReader(private val reader: BufferedReader) {
    private val lines = mutableListOf<String>()

    fun readStatement() : String? {
        while(true) {
            val line = reader.readLine()

            if (lines.isEmpty() && line == null) {
                return null
            } else if (lines.isEmpty() && line.contains(dateMatch)) {
                line ?: return null
                lines += line
            } else if (lines.isNotEmpty() && line == null) {
                val result = lines.joinToString("\n")
                lines.clear()
                return result
            } else if(lines.isNotEmpty() && line.contains(dateMatch)) {
                val result = lines.joinToString("\n")
                lines.clear()
                lines += line
                return result
            } else {
                lines += line
            }
        }
    }
}

data class LogParserOptions(val skipParseErrors: Boolean = false)

class LogParser(reader: BufferedReader, val options: LogParserOptions = LogParserOptions()) {
    private val logReader = LogReader(reader)
    private var currentLine: String? = null

    val line: String?
        get() = currentLine

    fun hasNext() : Boolean {
        currentLine = logReader.readStatement()
        currentLine ?: return false
        return true
    }

    fun next() : LogEntry {
        if (currentLine == null) {
            throw Exception("no currentLine read")
        }

        try {
            // Current currentLine
            val line = currentLine!!.trim()

            // Are we in debug mode
            if (logger.isDebugEnabled) {
                logger.debug(line)
            }

            // Start parsing itK
            val tokenizer = StringTokenizer(line, " ")
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
                // Check that we have a valid currentLine
                SeverityLevels.valueOf(severityLevel)
            } catch (ex: Exception) {
                throw Exception("not a legal MongoDB log currentLine entry [$line]")
            }

            // Process the types we know
            return when (logType.toUpperCase()) {
                LogTypeNames.COMMAND.name -> parseInfo(dateTime, SeverityLevels.valueOf(severityLevel), tokenizer)
                LogTypeNames.WRITE.name -> parseWrite(dateTime, SeverityLevels.valueOf(severityLevel), tokenizer)
                else -> NoSupportedLogEntry(line)
            }
        } catch (err: Exception) {
            if (options.skipParseErrors) {
                logger.info ("Failed to parse log statement: [$currentLine]", err)

                if (hasNext()) {
                    return next()
                } else {
                    throw Exception("no currentLine read")
                }
            } else {
                throw err
            }
        }
    }

    private fun parseWrite(dateTime: DateTime, severityLevel: SeverityLevels, tokenizer: StringTokenizer): LogEntry {
        // Read the connection information
        tokenizer.nextToken()
        // Skip the next token
        val commandName = tokenizer.nextToken().trim()
        // Read the namespace
        val namespaceString = tokenizer.nextToken()
        val namespace = Namespace.parse(namespaceString)

        // Split the currentLine into ever key we can
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

        // Check if it's been shortened (we can't parse this)
        if (restOfLine.contains(VALIDATE_SHORTENED)) {
            return NoSupportedLogEntry(currentLine!!)
        }

        // Create string tokenizer
        val partsTokenizer = StringTokenizer(restOfLine, " ")
        // Keep parsing until end
        val values = extractLogLineParts(partsTokenizer)
        // Update the value
        return entry.update(values)
    }

    private fun parseInfo(dateTime: DateTime, severityLevel: SeverityLevels, tokenizer: StringTokenizer): LogEntry {
        // Read the connection information
        tokenizer.nextToken()
        // Skip the next token
        tokenizer.nextToken()
        // Read the namespace
        val namespaceString = tokenizer.nextToken()
        val namespace = Namespace.parse(namespaceString)
        // We have a valid currentLine
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
        // Split the currentLine into ever key we can
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

        // Check if it's been shortened (we can't parse this)
        if (restOfLine.contains(VALIDATE_SHORTENED)) {
            return NoSupportedLogEntry(currentLine!!)
        }

        // Create string tokenizer
        val partsTokenizer = StringTokenizer(restOfLine, " ")
        // Keep parsing until end
        val values = extractLogLineParts(partsTokenizer)
        // Update the value
        return entry.update(values)
    }

    private fun extractLogLineParts(partsTokenizer: StringTokenizer) : MutableMap<String, Any> {
        var previousToken: String? = null
        val values = mutableMapOf<String, Any>(
            "command" to JsonObject()
        )

        while (partsTokenizer.hasMoreTokens()) {
            val token: String

            if (previousToken != null) {
                token = previousToken
                previousToken = null
            } else {
                token = partsTokenizer.nextToken()
            }

            if (token.startsWith("command:")) {
                val jsToken = partsTokenizer.nextToken()
                val json: String

                // Attempt to read the string as a json
                if (jsToken == "{") {
                    json = readJson(partsTokenizer, jsToken)
                } else {
                    values["commandName"] = jsToken
                    json = readJson(partsTokenizer)
                }

                // Check if the json file has been redacted
                values["command"] = commandToJsonObject(json)
            } else if (token.startsWith("appName:")) {
                val tokens = mutableListOf<String>()

                while (true) {
                    val value = partsTokenizer.nextToken()

                    if (value.last() == '"') {
                        tokens += value.substring(0, value.length - 1)
                        break
                    }

                    tokens += when (tokens.size) {
                        0 -> value.substring(1)
                        else -> value
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
            } else if (token.startsWith("ninserted:")) {
                values["ninserted"] = extractInt(token, partsTokenizer)
            } else if (token.startsWith("keysInserted:")) {
                values["keysInserted"] = extractInt(token, partsTokenizer)
            } else if (token.startsWith("reslen:")) {
                values["reslen"] = extractInt(token, partsTokenizer)
            } else if (token.startsWith("cursorExhausted:")) {
                values["cursorExhausted"] = extractInt(token, partsTokenizer)
            } else if (token.startsWith("queryHash:")) {
                values["queryHash"] = extractString(token, partsTokenizer)
            } else if (token.startsWith("protocol:")) {
                values["protocol"] = extractString(token, partsTokenizer)
            } else if (token.startsWith("errMsg:")) {
                val parts = token.split("errMsg:")
                val tokens = mutableListOf<String>()

                previousToken = readDoubleQuotedString(parts[1], partsTokenizer, previousToken, tokens)

                values["errMsg"] = tokens.joinToString(" ")
            } else if (token.startsWith("errName:")) {
                values["errName"] = extractString(token, partsTokenizer)
            } else if (token.startsWith("errCode:")) {
                values["errCode"] = extractInt(token, partsTokenizer)
            } else if (token.startsWith("locks:")) {
                if (token.contains("{") && token.contains("}")) {
                    values["locks"] = JsonObject()
                } else if (token.contains("{")) {
                    values["locks"] = commandToJsonObject(readJson(partsTokenizer, "{"))
                } else {
                    values["locks"] = commandToJsonObject(readJson(partsTokenizer))
                }
            } else if (token.startsWith("update:")) {
                val update = commandToJsonObject(readJson(partsTokenizer))

                if (values.containsKey("command")) {
                    (values.get("command") as JsonObject).put("u", update)
                }
            } else if (token.startsWith("query:")) {
                val query = commandToJsonObject(readJson(partsTokenizer))

                if (values.containsKey("command")) {
                    (values.get("command") as JsonObject).put("q", query)
                }
            } else if (token.startsWith("planSummary:")) {
                // Read until next tag
                val tokens = mutableListOf<String>()
                previousToken = readUntilNextTag(tokens, partsTokenizer)

                // If we have a single Token
                if (tokens.size == 1)  {
                    values["planSummary"] = PlanSummary(listOf(PlanSummaryEntry(tokens.first())))
                } else {
                    // Create the string to parse
                    val plan = tokens.joinToString(" ")

                    // Parse it
                    val tokenizer = StringTokenizer(plan, " ")
                    val entries = mutableListOf<PlanSummaryEntry>()

                    while (tokenizer.hasMoreTokens()) {
                        val nToken = tokenizer.nextToken()

                        if (nToken.contains("IXSCAN")) {
                            // Read the json
                            val json = readJson(tokenizer)
                            entries += PlanSummaryEntry("IXSCAN", commandToJsonObject(json))
                        }
                    }

                    values["planSummary"] = PlanSummary(entries)
                }
            } else if (token.startsWith("exception:")) {
                // Read until next tag
                val tokens = mutableListOf<String>()
                previousToken = readUntilNextTag(tokens, partsTokenizer, listOf("code", "reslen"))
                values["exception"] = tokens.joinToString(" ")
            } else if (token.startsWith("code:")) {
                values["code"] = extractInt(token, partsTokenizer)
            }
        }

        // Do we have an errName but no exception
        if (values.containsKey("errMsg") && !values.containsKey("exception")) {
            values["exception"] = values["errMsg"]!!
        }

        if (values.containsKey("errCode") && !values.containsKey("code")) {
            values["code"] = values["errCode"]!!
        }

        return values
    }

    private fun readUntilNextTag(tokens: MutableList<String>, tokenizer: StringTokenizer, untilTag: List<String> = listOf()) : String? {
        var previousToken: String? = null

        while (tokenizer.hasMoreTokens()) {
            val nextToken = tokenizer.nextToken()

            if (nextToken.startsWith("{")) {
                tokens += readJson(tokenizer, nextToken)
            }

            if (nextToken.contains(Regex("^(\\w+):"))) {
                val tag = Regex("^(\\w+):").find(nextToken)!!.groups[1]!!.value

                if (untilTag.isNotEmpty() && tag in untilTag) {
                    previousToken = nextToken
                    break
                } else if (untilTag.isEmpty() && tag in tags) {
                    previousToken = nextToken
                    break
                }
            }

            tokens += nextToken
        }

        return previousToken
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

            if (nextToken.contains("{")) {
                depth += nextToken.count {
                    it == '{'
                }
            }

            if (nextToken.contains("}")) {
                depth -= nextToken.count {
                    it == '}'
                }
            }

            tokens += nextToken
        } while (depth != 0 && partsTokenizer.hasMoreTokens())

        if (depth != 0) {
            throw Exception("illegal json string [${tokens.joinToString(" ")}]")
        }

        return tokens.joinToString(" ").substringBeforeLast("}") + "}"
    }

    fun forEach(function: (entry: LogEntry) -> Unit) {
        while (hasNext()) {
            try {
                function(next())
            } catch (err: Exception) {
                if (err.message != "no currentLine read") {
                    function(next())
                }
            }
        }
    }

    companion object : KLogging()
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
