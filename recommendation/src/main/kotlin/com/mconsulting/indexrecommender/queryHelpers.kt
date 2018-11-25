package com.mconsulting.indexrecommender

import jdk.nashorn.api.scripting.ScriptObjectMirror
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonElement
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonJavaScript
import org.bson.BsonRegularExpression
import org.bson.BsonString
import org.bson.BsonUndefined
import org.bson.BsonValue
import java.util.*
import javax.script.ScriptEngineManager
import kotlin.Exception
import kotlin.math.roundToLong

fun generateProjection(document: BsonDocument): BsonDocument {
    val paths = mutableListOf<String>()

    // Generate all the projections
    generatePaths(document, mutableListOf(), paths)

    // Return the BsonDocument for the projections
    return BsonDocument(paths.map {
        BsonElement(it, BsonInt32(1))
    })
}

fun generatePaths(document: BsonDocument, path: MutableList<String>, paths: MutableList<String>) {
    for (entry in document.entries) {
        val subPath = path.toMutableList()
        subPath += entry.key

        if (!entry.key.contains(".")) {
            val value = entry.value

            when (value) {
                is BsonDocument -> generatePaths(value, subPath, paths)
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

fun array(doc: BsonArray, path: MutableList<String>, func: (doc: BsonDocument, path: MutableList<String>, entry: MutableMap.MutableEntry<String, BsonValue?>) -> Any?) {
    doc.forEachIndexed { index, entry ->
        path.add(index.toString())

        when (entry) {
            is BsonArray -> array(entry, path, func)
            is BsonDocument -> document(entry, path, func)
        }

        path.removeAt(path.lastIndexOf(index.toString()))
    }
}

fun document(doc: BsonDocument, path: MutableList<String>, func: (doc: BsonDocument, path: MutableList<String>, entry: MutableMap.MutableEntry<String, BsonValue?>) -> Any?) {
    for (entry in doc.entries) {

        func(doc, path, entry)

        path.add(entry.key)

        val value = entry.value

        when (value) {
            is BsonArray -> array(value, path, func)
            is BsonDocument -> document(value, path, func)
        }

        path.removeAt(path.lastIndexOf(entry.key))
    }
}

fun traverse(doc: BsonDocument, func: (doc: BsonDocument, path: MutableList<String>, entry: MutableMap.MutableEntry<String, BsonValue?>) -> Any?) {
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
class Match(val value: String, val pattern: Pattern)
class Counter(var count: Int =  0) {
    fun inc() : Int {
        return count++
    }
}

private val ISO_REGEX = Pattern(Regex("""ISODate\(\"([\d|\w|\-|\:|\.]+)\"\)"""), "{ \"\\${'$'}date\": \"$1\" }")
private val OBJECTID_REGEX = Pattern(Regex("""ObjectId\(\"([\d|\w]+)\"\)"""), "{ \"\\${'$'}oid\": \"$1\" }")
private val NUMBERLONG_REGEX = Pattern(Regex("""NumberLong\(\"([\d]+)\"\)"""), "{ \"\\${'$'}numberLong\": \"$1\" }")
private val NUMBERINT_REGEX = Pattern(Regex("""NumberInt\(\"([\d]+)\"\)"""), "$1")
private val NUMBERDECIMAL_REGEX = Pattern(Regex("""NumberDecimal\(\"([\d|\.]+)\"\)"""), "{ \"\\${'$'}numberDecimal\": \"$1\" }")
private val BINDATA_REGEX = Pattern(Regex("""BinData\(([\d])+,[ ]*\"([\d|\w|\+|\/\=]+)\"\)"""), "{ \"\\${'$'}binary\": \"$2\", \"\\${'$'}type\": \"$1\" }")
private val TIMESTAMP_REGEX = Pattern(Regex("""Timestamp\(([\d])+,[ ]*([\d])+\)"""), "{ \"\\${'$'}timestamp\": { \"t\": $1, \"i\": $2 } }")
private val UNDEFINED_REGEX = Pattern(Regex("undefined"), "{ \"\\${'$'}undefined\": true }")
private val BOOLEAN_REGEX = Pattern(Regex("""Boolean\(\"(\w+)\"\)"""), "$1")
private val BOOLEAN2_REGEX = Pattern(Regex("""Boolean\((\w+)\)"""), "$1")
private val MINKEY_REGEX = Pattern(Regex("""MinKey|MinKey\(\)"""), "{ \"\\${'$'}minKey\": 1 }")
private val MAXKEY_REGEX = Pattern(Regex("""MaxKey|MaxKey\(\)"""), "{ \"\\${'$'}maxKey\": 1 }")
private val UUID_REGEXP = Pattern(Regex("""UUID\(\"([\d|\w|\-]+)\"\)"""), "$1")

// Javascript Engine
private val engine = ScriptEngineManager().getEngineByName("javascript")

//UUID("b8a51588-fc4d-4bfc-89e4-903ba7ffadc1")
fun commandToBsonDocument(json: String): BsonDocument {
    // Check if we need to parse the slow path
    if (!json.contains(Regex("\\:[ ]*function[ ]*\\("))) {
        return fastPath(json)
    } else {
        return slowPath(json)
    }
}

fun slowPath(json: String): BsonDocument {
    var finalJson = rewriteBsonTypes(json)
    val obj = engine.eval("result = $finalJson")
    obj ?: throw Exception("illegal json document: [$json]")

    if (obj !is ScriptObjectMirror) {
        throw Exception("illegal json document: [$json]")
    }

    // Rewrite script object to BsonDocument
    val result = translateScriptObject(obj)
    // Turn to JSON and then re-parse to correctly handle any extended JSON
    val finalDocument = BsonDocument.parse(result.toJson())
    return finalDocument
}

fun translateScriptObject(obj: ScriptObjectMirror) : BsonDocument {
    var document = BsonDocument()

    for (entry in obj.entries) {
        val value = entry.value
        val bsonValue:BsonValue = when(value) {
            is ScriptObjectMirror -> {
                if (value.isFunction) {
                    BsonJavaScript(value.toString())
                } else if (isRegularExpression(value)) {
                    mapToRegularExpression(value)
                } else {
                    translateScriptObject(value)
                }
            }
            is String -> BsonString(value)
            is Double -> {
                if ((value == Math.floor(value)) && !value.isInfinite()) {
                    BsonInt64(value.roundToLong())
                } else {
                    BsonDouble(value)
                }
            }
            is Int -> BsonInt32(value)
            else -> BsonUndefined()
        }

        document.append(entry.key, bsonValue)
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

fun fastPath(json: String): BsonDocument {
    var finalJson = rewriteBsonTypes(json)
    // Returned the processed tokens
    return BsonDocument.parse(finalJson)
}

private fun rewriteBsonTypes(finalJson: String): String {
    var finalJson1 = finalJson
    var index = Counter()
    val allMatches = mutableMapOf<String, Match>()

    // Replace all custom fields to ensure we can parse the JSON
    finalJson1 = extractCustomShellSyntax(ISO_REGEX, finalJson1, index, allMatches)
    finalJson1 = extractCustomShellSyntax(OBJECTID_REGEX, finalJson1, index, allMatches)
    finalJson1 = extractCustomShellSyntax(NUMBERLONG_REGEX, finalJson1, index, allMatches)
    finalJson1 = extractCustomShellSyntax(NUMBERINT_REGEX, finalJson1, index, allMatches)
    finalJson1 = extractCustomShellSyntax(NUMBERDECIMAL_REGEX, finalJson1, index, allMatches)
    finalJson1 = extractCustomShellSyntax(BINDATA_REGEX, finalJson1, index, allMatches)
    finalJson1 = extractCustomShellSyntax(TIMESTAMP_REGEX, finalJson1, index, allMatches)
    finalJson1 = extractCustomShellSyntax(UNDEFINED_REGEX, finalJson1, index, allMatches)
    finalJson1 = extractCustomShellSyntax(BOOLEAN_REGEX, finalJson1, index, allMatches)
    finalJson1 = extractCustomShellSyntax(BOOLEAN2_REGEX, finalJson1, index, allMatches)
    finalJson1 = extractCustomShellSyntax(MINKEY_REGEX, finalJson1, index, allMatches)
    finalJson1 = extractCustomShellSyntax(MAXKEY_REGEX, finalJson1, index, allMatches)
    finalJson1 = extractCustomShellSyntax(UUID_REGEXP, finalJson1, index, allMatches)

    // Get keys and sort in order of length
    val keys = allMatches.keys.sortedBy { -it.length }

    // Tokenize the json to rewrite some tokens
    val tokenizer = StringTokenizer(finalJson1, " ")
    val tokens = mutableListOf<String>()

    // Go over all the tokens
    while (tokenizer.hasMoreTokens()) {
        val token = tokenizer.nextToken()
        val match = Regex("([\\$|\\_|\\-|\\w+|0-9|\\.]+)\\:").find(token)

        if (token.startsWith('$')) {
            tokens += "\"${token.substringBeforeLast(":")}\":"
        } else if (match != null) {
            tokens += "\"${match.groups[1]!!.value}\":"
        } else {
            tokens += token
        }
    }

    finalJson1 = tokens.joinToString(" ")

    // Put back the Shell extensions
    for (key in keys) {
        if (allMatches[key]!!.pattern == UUID_REGEXP) {
            val match = UUID_REGEXP.match.find(allMatches[key]!!.value)
            if (match!!.groups.isNotEmpty()) {
                val data = match.groupValues.last()
                val encodedData = Base64.getEncoder().encodeToString(data.toByteArray())

                finalJson1 = finalJson1.replace("\"$key\"", allMatches[key]!!.value.replace(
                    allMatches[key]!!.pattern.match,
                    "{ \"\\${'$'}binary\": \"$encodedData\", \"\\${'$'}type\": \"4\" }"
                ))
            }
            //
        } else {
            finalJson1 = finalJson1.replace("\"$key\"", allMatches[key]!!.value.replace(
                allMatches[key]!!.pattern.match,
                allMatches[key]!!.pattern.replace
            ))
        }
    }
    return finalJson1
}

private fun extractCustomShellSyntax(pattern: Pattern, json: String, index: Counter, allMatches: MutableMap<String, Match>): String {
    // Rewrite the json to allow us to parse it correctly
    var finalJson = json
    val matches = pattern.match.findAll(finalJson)

    for (match in matches) {
        val key = "##${index.inc()}"
        allMatches[key] = Match(match.value, pattern)
        finalJson = finalJson.replace(match.value, "\"$key\"")
    }
    return finalJson
}
