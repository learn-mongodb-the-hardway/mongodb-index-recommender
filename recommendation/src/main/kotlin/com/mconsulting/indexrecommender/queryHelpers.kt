package com.mconsulting.indexrecommender

import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonElement
import org.bson.BsonInt32
import org.bson.BsonValue
import java.util.*

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

//UUID("b8a51588-fc4d-4bfc-89e4-903ba7ffadc1")

fun commandToBsonDocument(json: String): BsonDocument {
    var finalJson = json
    var index = Counter()
    val allMatches = mutableMapOf<String, Match>()

    // Replace all custom fields to ensure we can parse the JSON
    finalJson = extractCustomShellSyntax(ISO_REGEX, finalJson, index, allMatches)
    finalJson = extractCustomShellSyntax(OBJECTID_REGEX, finalJson, index, allMatches)
    finalJson = extractCustomShellSyntax(NUMBERLONG_REGEX, finalJson, index, allMatches)
    finalJson = extractCustomShellSyntax(NUMBERINT_REGEX, finalJson, index, allMatches)
    finalJson = extractCustomShellSyntax(NUMBERDECIMAL_REGEX, finalJson, index, allMatches)
    finalJson = extractCustomShellSyntax(BINDATA_REGEX, finalJson, index, allMatches)
    finalJson = extractCustomShellSyntax(TIMESTAMP_REGEX, finalJson, index, allMatches)
    finalJson = extractCustomShellSyntax(UNDEFINED_REGEX, finalJson, index, allMatches)
    finalJson = extractCustomShellSyntax(BOOLEAN_REGEX, finalJson, index, allMatches)
    finalJson = extractCustomShellSyntax(BOOLEAN2_REGEX, finalJson, index, allMatches)
    finalJson = extractCustomShellSyntax(MINKEY_REGEX, finalJson, index, allMatches)
    finalJson = extractCustomShellSyntax(MAXKEY_REGEX, finalJson, index, allMatches)
    finalJson = extractCustomShellSyntax(UUID_REGEXP, finalJson, index, allMatches)

    // Get keys and sort in order of length
    val keys = allMatches.keys.sortedBy { -it.length }

    // Put back the Shell extensions
    for (key in keys) {
        if (allMatches[key]!!.pattern == UUID_REGEXP) {
            val match = UUID_REGEXP.match.find(allMatches[key]!!.value)
            if (match!!.groups.isNotEmpty()) {
                val data = match.groupValues.last()
                val encodedData = Base64.getEncoder().encodeToString(data.toByteArray())

                finalJson = finalJson.replace("\"$key\"", allMatches[key]!!.value.replace(
                    allMatches[key]!!.pattern.match,
                    "{ \"\\${'$'}binary\": \"$encodedData\", \"\\${'$'}type\": \"4\" }"
                ))
            }
            //
        } else {
            finalJson = finalJson.replace("\"$key\"", allMatches[key]!!.value.replace(
                allMatches[key]!!.pattern.match,
                allMatches[key]!!.pattern.replace
            ))
        }
    }

    // Returned the processed tokens
    return BsonDocument.parse(finalJson)
}

private fun extractCustomShellSyntax(pattern: Pattern, finalJson: String, index: Counter, allMatches: MutableMap<String, Match>): String {
    // Rewrite the json to allow us to parse it correctly
    var finalJson = finalJson
    val matches = pattern.match.findAll(finalJson)

    for (match in matches) {
        val key = "##${index.inc()}"
        allMatches[key] = Match(match.value, pattern)
        finalJson = finalJson.replace(match.value, "\"$key\"")
    }
    return finalJson
}
