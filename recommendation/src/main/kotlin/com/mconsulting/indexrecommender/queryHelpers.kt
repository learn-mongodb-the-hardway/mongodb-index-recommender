package com.mconsulting.indexrecommender

import com.beust.klaxon.JsonArray
import com.beust.klaxon.JsonObject
import jdk.nashorn.api.scripting.ScriptObjectMirror
import mu.KotlinLogging
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonRegularExpression
import java.util.*
import javax.script.ScriptEngineManager

fun generateProjection(document: JsonObject): JsonObject {
    val paths = mutableListOf<String>()

    // Generate all the projections
    generatePaths(document, mutableListOf(), paths)

    // Return the BsonDocument for the projections
    return JsonObject(paths.map {
        it to 1
    }.toMap())
}

fun generatePaths(document: JsonObject, path: MutableList<String>, paths: MutableList<String>) {
    for (entry in document.entries) {
        val subPath = path.toMutableList()
        subPath += entry.key

        if (!entry.key.contains(".")) {
            val value = entry.value

            when (value) {
                is JsonObject -> generatePaths(value, subPath, paths)
                else -> paths.add(subPath.joinToString("."))
            }
        } else {
            paths.add(subPath.joinToString("."))
        }
    }
}

fun containsArray(doc: BsonDocument): Boolean {
    var contains = false

    for (entry in doc.entries) {
        val v = entry.value
        val value = when (v) {
            is BsonDocument -> containsArray(v)
            is BsonArray -> true
            else -> false
        }

        contains = contains.or(value)
    }

    return contains
}

fun array(doc: JsonArray<*>, path: MutableList<String>, func: (doc: JsonObject, path: MutableList<String>, entry: MutableMap.MutableEntry<String, Any?>) -> Any?) {
    doc.forEachIndexed { index, entry ->
        path.add(index.toString())

        when (entry) {
            is JsonArray<*> -> array(entry, path, func)
            is JsonObject -> document(entry, path, func)
        }

        path.removeAt(path.lastIndexOf(index.toString()))
    }
}

fun document(doc: JsonObject, path: MutableList<String>, func: (doc: JsonObject, path: MutableList<String>, entry: MutableMap.MutableEntry<String, Any?>) -> Any?) {
    for (entry in doc.entries.toList()) {

        func(doc, path, entry)

        path.add(entry.key)

        val value = entry.value

        when (value) {
            is JsonArray<*> -> array(value, path, func)
            is JsonObject -> document(value, path, func)
        }

        path.removeAt(path.lastIndexOf(entry.key))
    }
}

fun traverse(doc: JsonObject, func: (doc: JsonObject, path: MutableList<String>, entry: MutableMap.MutableEntry<String, Any?>) -> Any?) {
    document(doc, mutableListOf(), func)
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

class Pattern(val match: Regex, val replace: String)

private val ISO_REGEX = Pattern(Regex("""ISODate\([\"|']([\d|\w|\-|\:|\.]+)[\"|']\)"""), "{ \"\\${'$'}date\": \"$1\" }")
private val OBJECTID_REGEX = Pattern(Regex("""ObjectId\([\"|']([\d|\w]+)[\"|']\)"""), "{ \"\\${'$'}oid\": \"$1\" }")
private val NUMBERLONG_REGEX = Pattern(Regex("""NumberLong\([\"|']([\d]+)[\"|']\)"""), "{ \"\\${'$'}numberLong\": \"$1\" }")
private val NUMBERINT_REGEX = Pattern(Regex("""NumberInt\([\"|']([\d]+)[\"|']\)"""), "$1")
private val NUMBERDECIMAL_REGEX = Pattern(Regex("""NumberDecimal\([\"|']([\d|\.]+)[\"|']\)"""), "{ \"\\${'$'}numberDecimal\": \"$1\" }")
private val BINDATA_REGEX = Pattern(Regex("""BinData\(([\d])+,[ ]*[\"|']*([\d|\w|\+|\/\=]+)[\"|']*\)"""), "{ \"\\${'$'}binary\": \"$2\", \"\\${'$'}type\": \"$1\" }")
private val BINDATA2_REGEX = Pattern(Regex("""BinData\(([\d])+,[ ]*\)"""), "{ \"\\${'$'}binary\": \"\", \"\\${'$'}type\": \"$1\" }")
private val TIMESTAMP_REGEX = Pattern(Regex("""Timestamp\(([\d])+,[ ]*([\d])+\)"""), "{ \"\\${'$'}timestamp\": { \"t\": $1, \"i\": $2 } }")
private val TIMESTAMP2_REGEX = Pattern(Regex("""Timestamp[ ]*(\d+)\|(\d+)"""), "{ \"\\${'$'}timestamp\": { \"t\": $1, \"i\": $2 } }")
private val UNDEFINED_REGEX = Pattern(Regex("[[ ]+|\\:]undefined|^[ |,]*undefined[,| ]*$"), "{ \"\\${'$'}undefined\": true }")
private val BOOLEAN_REGEX = Pattern(Regex("""Boolean\([\"|'](\w+)[\"|']\)"""), "$1")
private val BOOLEAN2_REGEX = Pattern(Regex("""Boolean\((\w+)\)"""), "$1")
private val MINKEY_REGEX = Pattern(Regex("""MinKey|MinKey\(\)"""), "{ \"\\${'$'}minKey\": 1 }")
private val MAXKEY_REGEX = Pattern(Regex("""MaxKey|MaxKey\(\)"""), "{ \"\\${'$'}maxKey\": 1 }")
private val UUID_REGEXP = Pattern(Regex("""UUID\([\"|']([\d|\w|\-]+)[\"|']\)"""), "$1")
//token.contains(Regex("Date\\([\\d]+\\)"))
private val DATE_REGEXP = Pattern(Regex("Date\\((\\d+)\\)"), "{ \"\\${'$'}numberLong\": \"$1\" }")

// Javascript Engine
private val engine = ScriptEngineManager().getEngineByName("javascript")

private val logger = KotlinLogging.logger { }

fun commandToJsonObject(json: String): JsonObject {
    return slowPath(json)
}

fun slowPath(json: String): JsonObject {
    var finalJson = rewriteBsonTypes(json)
    val obj = engine.eval("result = $finalJson")
    obj ?: throw Exception("illegal json document: [$json]")

    if (obj !is ScriptObjectMirror) {
        throw Exception("illegal json document: [$json]")
    }

    // Rewrite script object to BsonDocument
    return translateScriptObject(obj)
}

fun translateScriptObject(obj: ScriptObjectMirror) : JsonObject {
    var document = JsonObject()

    for (entry in obj.entries) {
        val value = entry.value
        val jsonValue = when(value) {
            is ScriptObjectMirror -> {
                if (value.isFunction) {
                    JsonObject(mapOf(
                        "\$code" to value.toString()
                    ))
                } else if (value.isArray) {
                    JsonArray(value.values.map {
                        when (it) {
                            is ScriptObjectMirror -> translateScriptObject(it)
                            else -> it
                        }
                    })
                } else if (isRegularExpression(value)) {
                    val regexValue = mapToRegularExpression(value)
                    JsonObject(mapOf(
                        "\$regex" to regexValue.pattern,
                        "\$options" to regexValue.options
                    ))
                } else {
                    translateScriptObject(value)
                }
            }
            else -> value
        }

        document[entry.key] = jsonValue
    }

    return document
}

fun mapToRegularExpression(value: ScriptObjectMirror): BsonRegularExpression {
    var options = ""

    if (value["ignoreCase"] as Boolean) {
        options += "i"
    }

    if (value["multiline"] as Boolean) {
        options += "m"
    }

    return BsonRegularExpression(value["source"] as String, options)
}

fun isRegularExpression(value: ScriptObjectMirror): Boolean {
    return value.entries.isEmpty()
        && value.containsKey("global")
        && value.containsKey("ignoreCase")
        && value.containsKey("multiline")
        && value.containsKey("source")
}

//fun fastPath(json: String): JsonObject {
//    var finalJson = rewriteBsonTypes(json)
//    return Parser().parse(StringReader(finalJson)) as JsonObject
//}

val DBRefRegex = Regex("""DBRef\((([\w|[a-z]|\'|\"|\${'$'}|\-|\_|\:| ])+,)+[ ]*([\w|[a-z]|\'|\"|\${'$'}|\-|\_|\:| ])+\)""")

private fun rewriteBsonTypes(json: String): String {
    var finalJson = json

    // Replace some basics
    finalJson = finalJson.replace(BINDATA_REGEX.match, BINDATA_REGEX.replace)
    finalJson = finalJson.replace(BINDATA2_REGEX.match, BINDATA2_REGEX.replace)
    finalJson = finalJson.replace(TIMESTAMP_REGEX.match, TIMESTAMP_REGEX.replace)
    finalJson = finalJson.replace(TIMESTAMP2_REGEX.match, TIMESTAMP2_REGEX.replace)
    finalJson = finalJson.replace(NUMBERLONG_REGEX.match, NUMBERLONG_REGEX.replace)
    finalJson = finalJson.replace(NUMBERDECIMAL_REGEX.match, NUMBERDECIMAL_REGEX.replace)
    finalJson = finalJson.replace(OBJECTID_REGEX.match, OBJECTID_REGEX.replace)
    finalJson = finalJson.replace(ISO_REGEX.match, ISO_REGEX.replace)
    finalJson = finalJson.replace(NUMBERINT_REGEX.match, NUMBERINT_REGEX.replace)
    finalJson = finalJson.replace(BOOLEAN_REGEX.match, BOOLEAN_REGEX.replace)
    finalJson = finalJson.replace(BOOLEAN2_REGEX.match, BOOLEAN2_REGEX.replace)
    finalJson = finalJson.replace(MINKEY_REGEX.match, MINKEY_REGEX.replace)
    finalJson = finalJson.replace(MAXKEY_REGEX.match, MAXKEY_REGEX.replace)

    // Replace any DBRef instances
    finalJson = rewriteDbRef(finalJson)

    // Tokenize the json to rewrite some tokens
    val tokenizer = StringTokenizer(finalJson, " ")
    val tokens = mutableListOf<String>()
    var previousToken: String? = null
    var leftOverToken: String? = null

    // Go over all the tokens
    while (tokenizer.hasMoreTokens()) {
        var token: String

        if (leftOverToken != null) {
            token = leftOverToken
            leftOverToken = null
        } else {
            token = tokenizer.nextToken()
        }

        if (token.contains("-inf.0")) {
            tokens += token.replace("-inf.0", "\"-inf.0\"")
        } else if (token.contains("inf.0")) {
            tokens += token.replace("inf.0", "\"inf.0\"")
        } else if (token.contains("nan.0")) {
            tokens += token.replace("nan.0", "\"nan.0\"")
        } else if (token.contains("-Infinity")) {
            tokens += token.replace("-Infinity", "\"-Infinity\"")
        } else if (token.contains("Infinity")) {
            tokens += token.replace("Infinity", "\"Infinity\"")
        } else if (token.contains("NaN")) {
            tokens += token.replace("NaN", "\"NaN\"")
        } else if (token.contains(UNDEFINED_REGEX.match)) {
            tokens += token.replace("undefined", "{ \"\$undefined\": true }")
        } else if (token.contains(UUID_REGEXP.match)) {
            // Grab group so we can base encode it
            val match = UUID_REGEXP.match.find(token)
            if (match!!.groups.isNotEmpty()) {
                val data = match.groupValues.last()
                val encodedData = Base64.getEncoder().encodeToString(data.toByteArray())
                tokens += token.replace(UUID_REGEXP.match, "{ \"\\${'$'}binary\" : \"$encodedData\", \"\\${'$'}type\" : \"04\" }")
            }
        } else if (token.contains(DATE_REGEXP.match)) {
            if (previousToken == "new") {
                tokens.removeAt(tokens.lastIndex)
            }

            tokens +=  token.replace(DATE_REGEXP.match, DATE_REGEXP.replace)
        } else if (token.startsWith('$')) {
            tokens += "\"${token.substringBeforeLast(":")}\":"
        } else if (token.contains(Regex("""^\d+e\-\d+"""))) {
            tokens += "\"$token\""
        } else if (token.contains(Regex("""^[\d|\.]*[e|E][\-|\+]\d+"""))) {
            tokens += "{ \"\$numberDecimal\": \"$token\" }"
        } else if (token.contains(Regex("([\\w|\\\$|\\_|\\-|\\d|$|\\[||\\]]+\\.)+[\\w|\\\$|\\_|\\-|\\d|\$|\\[||\\]]+:"))) {
            tokens += "\"${token.substringBeforeLast(":")}\":"
        } else if (token.contains(Regex("^\\d")) && token.endsWith(":")) {
            tokens += "\"${token.substringBeforeLast(":")}\":"
        } else if (!token.contains("\"") && token.contains("-") && token.endsWith(":")) {
            tokens += "\"${token.substringBeforeLast(":")}\":"
        } else if (token.startsWith("\"")) {
            leftOverToken = readDoubleQuotedString(token, tokenizer, leftOverToken, tokens)
        } else {
            tokens += token
        }

        previousToken = token
    }

    val finalResult = tokens.joinToString(" ")
    return finalResult
}

fun readDoubleQuotedString(token: String, tokenizer: StringTokenizer, leftOverToken: String?, tokens: MutableList<String>): String? {
    // Previous character
    var leftOverToken1 = leftOverToken
    var previousChar: Char = ' '
    // Count the number of unescaped quotes
    val numberOfDoubleQuotes = token.count {
        if (it == '"' && previousChar != '\\') {
            true
        } else {
            previousChar = it
            false
        }
    }

    // We need to read the string until the next double quote
    if (numberOfDoubleQuotes == 1) {
        val stringTokens = mutableListOf(token.replace("\n", """\n"""))

        while (true && tokenizer.hasMoreTokens()) {
            var nextToken = tokenizer.nextToken().replace("\n", """\n""")

            // Did we find the end of the string
            if (nextToken.contains("\"")) {
                stringTokens += nextToken.substringBeforeLast("\"") + "\""
                leftOverToken1 = nextToken.substringAfterLast("\"")
                break
            } else {
                stringTokens += nextToken
            }
        }
        tokens += stringTokens.joinToString(" ")
    } else if (numberOfDoubleQuotes == 2) {
        tokens += token.replace("\n", """\n""")
    } else {
        val tok = token
            .substringAfter("\"")
            .substringBeforeLast("\"")
            .replace("\"", "\\\"")
        tokens += ("\"$tok\"" + token.substringAfterLast("\"")).replace("\n", """\n""")
    }
    return leftOverToken1
}

private fun rewriteDbRef(finalJson: String): String {
    var finalJson1 = finalJson
    while (finalJson1.contains(DBRefRegex)) {
        val match = DBRefRegex.find(finalJson1)
        val string = match!!.value
        val paramsString = string.substringAfter("DBRef(").substringBeforeLast(")")
        val params = paramsString.split(",").map { it.trim() }

        // Grab the collection name and id
        val collectionName = params[0].replace("'", "\"")
        val id = params[1]
        val parts = mutableListOf("\"\$ref\": $collectionName")

        // [^[\d|a-f|A-F]+$|^[\"|\'][\d|a-f|A-F]+[\"|\'$}]]
        // Replace the id if it's freestanding
        if (id.matches(Regex("""(^[\d|a-f|A-F]+${'$'})"""))) {
            parts += "\"\$id\": { \"\$oid\": \"$id\" }"
        } else if (id.matches(Regex("""(^["|'][\d|a-f|A-F]+["|']${'$'})"""))) {
            parts += "\"\$id\": { \"\$oid\": \"${id.replace("'", "\"")}\" }"
        }

        // Do we have the $db field
        if (parts.size == 3) {
            parts += "\"\$db\": ${parts[2].replace("'", "\"")}"
        }

        finalJson1 = finalJson1.replaceFirst(string, " { ${parts.joinToString(", ")} }")
    }
    return finalJson1
}
