package com.mconsulting.indexrecommender.indexes

import com.mconsulting.indexrecommender.queryplans.QueryPlan
import com.mongodb.MongoClient
import org.bson.BsonDocument
import org.bson.BsonElement
import org.bson.BsonInt32
import org.bson.Document
import org.bson.json.JsonWriterSettings

abstract class Index(val sparse: Boolean = false, val unique: Boolean = false)

enum class IndexDirection {
    ASCENDING, DESCENDING, UNKNOWN;

    fun value() : Int {
        return when (this) {
            ASCENDING -> 1
            DESCENDING -> -1
            UNKNOWN -> throw Exception("cannot convert index direction <UNKNOWN> into MongoDB direction")
        }
    }

    companion object {
        fun intValueOf(value: Int) : IndexDirection {
            return when (value) {
                -1 -> IndexDirection.DESCENDING
                1 -> IndexDirection.ASCENDING
                else -> IndexDirection.UNKNOWN
            }
        }
    }
}

data class Field(val name: String, val direction: IndexDirection)

class IdIndex : Index(unique = true)

class SingleFieldIndex(val field: Field, sparse: Boolean = false, unique: Boolean = false) : Index(sparse, unique)

class CompoundIndex(val fields: List<Field>, sparse: Boolean = false, unique: Boolean = false) : Index(sparse, unique)

class MultikeyIndex(val fields: List<Field>, sparse: Boolean = false, unique: Boolean = false) : Index(sparse, unique)

abstract class GeospatialIndex : Index()

class TwoDSphereIndex : GeospatialIndex()

class TwoDIndex: GeospatialIndex()

class TextIndex : Index()

class HashedIndex(val field: String) : Index()

class PartialIndex : Index()

class TTLIndex : Index()

data class IndexParserOptions(val allowExplainExecution: Boolean = false)

class IndexParser(val client: MongoClient?, val options: IndexParserOptions = IndexParserOptions()) {

    fun createIndex(document: BsonDocument) : Index {
        println(document.toJson(JsonWriterSettings.builder().indent(true).build()))

        if (document.containsKey("key")) {
            val key = document.getDocument("key")
            var unique = false
            var sparse = false

            // Extract the unique, sparse flags
            if (document.containsKey("unique")) {
                unique = document.getBoolean("unique").value
            }

            if (document.containsKey("sparse")) {
                sparse = document.getBoolean("sparse").value
            }

            // Do we allow explain execution
            val queryPlan = extractQueryPlan(document, key)

            // Single field index
            if (key.size == 1) {
                // Check if the key is hashed
                if (key.isString(key.firstKey) && key.getString(key.firstKey).value.toLowerCase() == "hashed") {
                    return HashedIndex(key.firstKey)
                }

                // Check if it's an id index
                if (key.containsKey("_id")) {
                    return IdIndex()
                }

                // Is it actually a multikey index
                if (queryPlan != null && queryPlan.isMultiKey()) {
                    return MultikeyIndex(
                        key.entries.map { Field(it.key, IndexDirection.intValueOf((it.value as BsonInt32).value)) },
                        sparse, unique)
                }

                // Otherwise return regular single field
                return SingleFieldIndex(Field(
                    key.firstKey,
                    IndexDirection.intValueOf(key.getInt32(key.firstKey).value)),
                    sparse, unique)
            } else if (key.size > 1) {

                // TODO: Check if we have multikey indexes (we need to query using this index and look at the explain plan)
                // to detect if it's a multikey index
                if (queryPlan != null && queryPlan.isMultiKey()) {
                    return MultikeyIndex(
                        key.entries.map { Field(it.key, IndexDirection.intValueOf((it.value as BsonInt32).value)) },
                        sparse, unique)
                }

                // Otherwise return a compound index
                return CompoundIndex(
                    key.entries.map { Field(it.key, IndexDirection.intValueOf((it.value as BsonInt32).value)) },
                    sparse, unique)
            }
        }

        return TTLIndex()
    }

    private fun extractQueryPlan(document: BsonDocument, key: BsonDocument?) : QueryPlan? {
        if (options.allowExplainExecution && client != null) {
            // Get the full namespace
            val ns = document.getString("ns")
            // Split the string into database and collection
            val parts = ns.value.split(".")
            val dbName = parts.first()
            val collectionName = parts.subList(1, parts.size).joinToString(".")

            // Get the collection
            val db = client.getDatabase(dbName)

            // Creat explain command
            val explainCommand = Document(mapOf(
                "explain" to Document(mapOf(
                    "find" to collectionName,
                    "filter" to Document(),
                    "sort" to key
                ))
            ))

            println(explainCommand.toJson(JsonWriterSettings.builder().indent(true).build()))

            // Find first document using the index and run the explain
            val result = db.runCommand(explainCommand, BsonDocument::class.java)

            println(result.toJson(JsonWriterSettings.builder().indent(true).build()))

            return QueryPlan(result)
        }

        return null
    }
}