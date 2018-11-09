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

fun array(doc: BsonArray, parent: BsonValue?, func: (doc: BsonDocument, parent: BsonValue?, entry: MutableMap.MutableEntry<String, BsonValue?>) -> Boolean) : Boolean {
    var contains = false

    for (entry in doc) {
        contains = contains.or(when (entry) {
            is BsonArray -> array(entry, doc, func)
            is BsonDocument -> document(entry, doc, func)
            else -> contains
        })
    }

    return contains
}

fun document(doc: BsonDocument, parent: BsonValue?, func: (doc: BsonDocument, parent: BsonValue?, entry: MutableMap.MutableEntry<String, BsonValue?>) -> Boolean) : Boolean {
    var contains = false

    for (entry in doc.entries) {
        contains = contains.or(func(doc, parent, entry))

        val value = entry.value

        contains = contains.or(when (value) {
            is BsonArray -> array(value, doc, func)
            is BsonDocument -> document(value, doc, func)
            else -> contains
        })
    }

    return contains
}

fun containsGeoSpatialPredicate(doc: BsonDocument, func: (doc: BsonDocument, parent: BsonValue?, entry: MutableMap.MutableEntry<String, BsonValue?>) -> Boolean): Boolean {
    return document(doc, null, func)
}
