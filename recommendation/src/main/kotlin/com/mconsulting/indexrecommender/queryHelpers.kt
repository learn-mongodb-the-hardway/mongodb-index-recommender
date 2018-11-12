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

private val ISO_REGEX = Regex("""ISODate\(\"([\d|\w|\-|\:|\.]+)\"\)""")
private val KEY_REGEX = Regex("^([\\d|\\w|\\-|]+)\\:")

fun commandToBsonDocument(json: String): BsonDocument {
    // Go over all the tokens
    val tokenizer = StringTokenizer(json, " ")
    val tokens = mutableListOf<String>()

    while (tokenizer.hasMoreTokens()) {
        val token = tokenizer.nextToken()

        if (token.matches(ISO_REGEX)) {
            tokens += token.replace(ISO_REGEX, "{ \"\\${'$'}date\": \"$1\" }")
        } else if (token.matches(KEY_REGEX)) {
            tokens += token.replace(KEY_REGEX, "\"$1\":")
        } else {
            tokens += token
        }
    }


//    // Rewrite all the keys to be json compatible
//    var modifiedJson = json.replace(Regex("""([\d|\w|\$|\.|\_]+)\:"""), "\"$1\":")
//    // Rewrite any UUID field as a bson type
//    modifiedJson = modifiedJson.replace(
//        Regex("""UUID\("([\d|\w|\-]+)"\)"""),
//        "{\"\\${'$'}binary\": \"$1\", \"\\${'$'}type\": \"4\"}")
//    // Rewrite any ISODate fields ISODate("2012-12-19T06:01:17.171Z")
////    modifiedJson = modifiedJson.replace(
////        Regex("""ISODate\("([\d|\w|\-|\:]+)"\)"""),
////        "{\"\\${'$'}binary\": \"$1\", \"\\${'$'}type\": \"4\"}")
//
//    println(modifiedJson)

    // TokenJSON
    val finalJson = tokens.joinToString(" ")

    // Returned the processed tokens
    return BsonDocument.parse(finalJson)
}
