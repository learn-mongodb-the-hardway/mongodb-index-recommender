package com.mconsulting.indexrecommender

import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonElement
import org.bson.BsonInt32
import org.bson.BsonValue

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
