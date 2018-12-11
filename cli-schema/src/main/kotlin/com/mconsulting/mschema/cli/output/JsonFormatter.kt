package com.mconsulting.mschema.cli.output

import com.beust.klaxon.JsonArray
import com.beust.klaxon.JsonObject
import com.mconsulting.indexrecommender.DbIndexResult
import com.mconsulting.mschema.cli.IndentationWriter
import com.mconsulting.mschema.cli.writeln
import org.bson.BsonArray
import org.bson.BsonBoolean
import org.bson.BsonDateTime
import org.bson.BsonDocument
import org.bson.BsonElement
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.json.JsonWriterSettings

class JsonFormatter : Formatter() {
    override fun render(db: DbIndexResult, writer: IndentationWriter) {
        val collections = BsonArray(db.collectionIndexResults.map { collection ->
            BsonDocument()
                .append("collection", BsonString(collection.namespace.collection))
                .append("statistics", BsonDocument(listOf(
                    BsonElement("document_count", BsonInt64(collection.collectionStats.count)),
                    BsonElement("index_count", BsonInt32(collection.indexes.size))
                )))
                .append("indexes", BsonArray(collection.indexes.map { index ->
                    val indexDocument = BsonDocument()
                        .append("name", BsonString(index.name))
                        .append("index_type", BsonString(indexTypeName(index)))
                        .append("filter", writeIndexSpecificJson(index))
                        .append("existing_index", BsonBoolean(index.isExistingIndex()))
                        .append("unique", BsonBoolean(index.sparse))
                        .append("sparse", BsonBoolean(index.sparse))
                        .append("statistics", when (index.isExistingIndex()) {
                            true -> BsonDocument()
                                .append("count", BsonInt64(index.indexStatistics!!.ops))
                                .append("since", BsonDateTime(index.indexStatistics!!.since.time))
                            false -> BsonDocument()
                                .append("count", BsonInt64(index.statistics.map { it.count }.sum()))
                        })
                        .append("shapes", BsonArray(index.statistics.map {
                            BsonDocument()
                                .append("filter", BsonDocument.parse(when (it.shape) {
                                    is JsonObject -> JsonObject(mapOf(
                                        "query" to it.shape
                                    ))
                                    is JsonArray<*> -> JsonObject(mapOf(
                                        "aggregation" to it.shape
                                    ))
                                    else -> JsonObject()
                                }.toJsonString()))
                                .append("count", BsonInt64(it.count))
                        }))
                        .append("removed_indexes", BsonArray(index.removedIndexes.map { removedIndex ->
                            BsonDocument()
                                .append("name", BsonString(removedIndex.name))
                                .append("index_type", BsonString(indexTypeName(removedIndex)))
                        }))

                    // Add partial expression if available
                    if (index.partialFilterExpression != null) {
                        indexDocument.append("partial_expression", BsonDocument.parse(index.partialFilterExpression!!.toJson()))
                    }

                    indexDocument
                }))
        })

        // Add the collections
        val dbDocument = BsonDocument()
            .append("db", BsonString(db.namespace.db))
            .append("collections", collections)

        // Write the output
        writer.writeln(dbDocument.toJson(JsonWriterSettings.builder().indent(true).build()))
    }
}