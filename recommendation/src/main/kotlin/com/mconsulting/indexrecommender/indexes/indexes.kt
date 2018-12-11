package com.mconsulting.indexrecommender.indexes

import com.mconsulting.indexrecommender.ShapeStatistics
import com.mconsulting.indexrecommender.queryplans.QueryPlan
import com.mongodb.MongoClient
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.BsonValue
import org.bson.Document
import java.util.*

data class IndexStatistics(val ops: Long, val since: Date)

abstract class Index(
    val name: String,
    val sparse: Boolean = false,
    val unique: Boolean = false,
    val partialFilterExpression: BsonDocument? = null,
    var indexStatistics: IndexStatistics? = null) {
    val statistics: MutableList<ShapeStatistics> = mutableListOf()
    val removedIndexes: MutableList<Index> = mutableListOf()

    fun isExistingIndex() : Boolean = indexStatistics != null
}

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

data class Field(val name: String, val direction: IndexDirection) {
    override fun equals(other: Any?): Boolean {
        if (other == null) return false
        if (other !is Field) return false
        if (other.name != name) return false
        return true
    }
}

class IdIndex(name: String, partialFilterExpression: BsonDocument? = null) : Index(name = name, unique = true, partialFilterExpression = partialFilterExpression) {
    override fun equals(other: Any?): Boolean {
        if (other == null) return false
        if (other !is IdIndex) return false
        if (other.name != name) return false
        return true
    }
}

class SingleFieldIndex(name: String, val field: Field, sparse: Boolean = false, unique: Boolean = false, partialFilterExpression: BsonDocument? = null) : Index(name, sparse, unique, partialFilterExpression) {
    override fun equals(other: Any?): Boolean {
        if (other == null) return false
        if (other !is SingleFieldIndex) return false
        if (other.field.name != this.field.name) return false
        if (other.field.direction != this.field.direction) return false
        return true
    }
}

abstract class MultiFieldIndex(name: String, val fields: List<Field>, sparse: Boolean = false, unique: Boolean = false, partialFilterExpression: BsonDocument? = null)  : Index(name, sparse, unique, partialFilterExpression)

class CompoundIndex(name: String, fields: List<Field>, sparse: Boolean = false, unique: Boolean = false, partialFilterExpression: BsonDocument? = null) : MultiFieldIndex(name, fields, sparse, unique, partialFilterExpression) {
    override fun equals(other: Any?): Boolean {
        if (other == null) return false
        if (other !is CompoundIndex) return false
        if (other.fields.size != this.fields.size) return false

        // Check that all fields are the same
        for (field in other.fields) {
            if (!this.fields.contains(field)) return false
        }

        return true
    }
}

class MultikeyIndex(name: String, fields: List<Field>, sparse: Boolean = false, unique: Boolean = false, partialFilterExpression: BsonDocument? = null) : MultiFieldIndex(name, fields, sparse, unique, partialFilterExpression) {
    override fun equals(other: Any?): Boolean {
        if (other == null) return false
        if (other !is MultikeyIndex) return false
        if (other.fields.size != this.fields.size) return false

        // Check that all fields are the same
        for (field in other.fields) {
            if (!this.fields.contains(field)) return false
        }

        return true
    }
}

abstract class GeospatialIndex(name: String, partialFilterExpression: BsonDocument? = null) : Index(name, partialFilterExpression = partialFilterExpression)

class TwoDSphereIndex(name: String, val key: String, partialFilterExpression: BsonDocument? = null) : GeospatialIndex(name, partialFilterExpression = partialFilterExpression) {
    override fun equals(other: Any?): Boolean {
        if (other == null) return false
        if (other !is TwoDSphereIndex) return false
        if (key != other.key) return false
        return true
    }
}

class TwoDIndex(name: String, val key: String, partialFilterExpression: BsonDocument? = null) : GeospatialIndex(name, partialFilterExpression = partialFilterExpression) {
    override fun equals(other: Any?): Boolean {
        if (other == null) return false
        if (other !is TwoDIndex) return false
        if (key != other.key) return false
        return true
    }
}

data class TextField(val path: List<String>, val weight: Int = 1)

open class TextIndex(name: String, val fields: List<TextField>, partialFilterExpression: BsonDocument? = null) : Index(name, partialFilterExpression = partialFilterExpression) {
    override fun equals(other: Any?): Boolean {
        if (other == null) return false
        if (other !is TextIndex) return false
        if (other.fields.size != this.fields.size) return false
        for (field in this.fields) {
            if (!other.fields.contains(field)) return false
        }

        return true
    }
}

class CompoundTextIndex(name: String, val compoundFields: List<Field>, textIndexFields: List<TextField>, partialFilterExpression: BsonDocument? = null) : TextIndex(name, textIndexFields, partialFilterExpression) {
    override fun equals(other: Any?): Boolean {
        if (other == null) return false
        if (other !is CompoundTextIndex) return false
        if (other.fields.size != this.fields.size) return false
        for (field in this.compoundFields) {
            if (!other.compoundFields.contains(field)) return false
        }
        for (field in this.fields) {
            if (!other.fields.contains(field)) return false
        }

        return true
    }
}

class HashedIndex(name: String, val field: String, partialFilterExpression: BsonDocument? = null) : Index(name, partialFilterExpression = partialFilterExpression)

class TTLIndex(name: String, val field: Field, val expireAfterSeconds: Int, partialFilterExpression: BsonDocument? = null) : Index(name, partialFilterExpression = partialFilterExpression)

data class IndexParserOptions(val allowExplainExecution: Boolean = false)

class IndexParser(val client: MongoClient?, private val options: IndexParserOptions = IndexParserOptions()) {

    fun createIndex(document: BsonDocument) : Index {
        var partialFilterExpression: BsonDocument? = null

        // Get partial index expression
        if (document.containsKey("partialFilterExpression")) {
            partialFilterExpression = document.getDocument("partialFilterExpression")
        }

        // Check if we have a text index
        if (isTextIndex(document)) {
            // Check if the index is CompoundTextIndex or normal TextIndex
            if (isCompoundTextIndex(document)) {
                return createCompoundTextIndex(document, partialFilterExpression)
            }

            return createTextIndex(document, partialFilterExpression)
        }

        // Check if we have a geospatial index
        if (isGeoSpatialIndex(document)) {
            return createGeoSpatialIndex(document, partialFilterExpression)
        }

        // Check if we have a hashed index
        if (isHashedIndex(document)) {
            return createHashedIndex(document, partialFilterExpression)
        }

        // Check if we have a primary key index
        if (isPrimaryKeyIndex(document)) {
            return createPrimaryKeyIndex(document, partialFilterExpression)
        }

        // Check if we have a ttl index
        if (isTTLIndex(document)) {
            return createTTLIndex(document, partialFilterExpression)
        }

        // The rest of the indexes
        if (document.containsKey("key")) {
            val key = document.getDocument("key")
            val name = document.getString("name")
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

            // Return index
            return createSingleCompoundOrMultiKeyIndex(document, queryPlan, name, key, sparse, unique, partialFilterExpression)
        }

        throw Exception("does not support the index type in [${document.toJson()}]")
    }

    private fun createCompoundTextIndex(document: BsonDocument, partialFilterExpression: BsonDocument?): Index {
        // Do we have more than two key entries (_fts, _ftsx)
        val fields = document.getDocument("key")!!.entries.map {
            Field(it.key, when (it.value) {
                is BsonInt32 -> IndexDirection.intValueOf(getInt32(it.value))
                else -> IndexDirection.UNKNOWN
            })
        }

        return CompoundTextIndex(
            document.getString("name").value,
            fields,
            document.getDocument("weights").map {
                TextField(listOf(it.key), getInt32(it.value))
            }, partialFilterExpression)
    }

    private fun isCompoundTextIndex(document: BsonDocument): Boolean {
        return document.getDocument("key")!!.size > 2
    }

    private fun createTTLIndex(document: BsonDocument, partialFilterExpression: BsonDocument?): Index {
        val keyDocument = document.getDocument("key")
        val key = keyDocument.firstKey

        return TTLIndex(
            document.getString("name").value,
            Field(key, IndexDirection.intValueOf(getInt32(keyDocument[key]))),
            document.getInt32("expireAfterSeconds").value,
            partialFilterExpression)
    }

    private fun isTTLIndex(document: BsonDocument): Boolean {
        return document.containsKey("expireAfterSeconds")
    }

    private fun getInt32(value: BsonValue?) : Int {
        if (value == null) return 0
        return when (value) {
            is BsonInt32 -> value.value
            is BsonInt64 -> value.value.toInt()
            is BsonDouble -> value.value.toInt()
            else -> 0
        }
    }

    private fun createSingleCompoundOrMultiKeyIndex(document: BsonDocument, queryPlan: QueryPlan?, name: BsonString, key: BsonDocument, sparse: Boolean, unique: Boolean, partialFilterExpression: BsonDocument?): Index {
        if (queryPlan != null
            && queryPlan.isMultiKey()
            && queryPlan.indexName() == name.value) {
            return MultikeyIndex(
                document.getString("name").value,
                key.entries.map { Field(it.key, IndexDirection.intValueOf(getInt32(it.value))) },
                sparse, unique, partialFilterExpression)
        }

        return when (key.size) {
            1 -> SingleFieldIndex(document.getString("name").value, Field(
                key.firstKey,
                IndexDirection.intValueOf(getInt32(key[key.firstKey]))),
                sparse, unique, partialFilterExpression)
            else -> CompoundIndex(document.getString("name").value,
                key.entries.map { Field(it.key, IndexDirection.intValueOf(getInt32(it.value))) },
                sparse, unique, partialFilterExpression)
        }
    }

    private fun createPrimaryKeyIndex(document: BsonDocument, partialFilterExpression: BsonDocument?): Index {
        var index: Index? = null

        if (document.getDocument("key").containsKey("_id") && document.getDocument("key").size == 1) {
            index = IdIndex(document.getString("name").value, partialFilterExpression)
        }

        return index!!
    }

    private fun isPrimaryKeyIndex(document: BsonDocument): Boolean {
        if (document.getDocument("key").containsKey("_id") && document.getDocument("key").size == 1) {
            return true
        }

        return false
    }

    private fun createHashedIndex(document: BsonDocument, partialFilterExpression: BsonDocument?): Index {
        var index: Index? = null

        for (entry in document.getDocument("key").entries) {
            index = when (entry.value.asString().value) {
                "hashed" -> HashedIndex(document.getString("name").value, entry.key, partialFilterExpression)
                else -> null
            }

            if (index != null) break
        }

        return index!!
    }

    private fun isHashedIndex(document: BsonDocument): Boolean {
        for (entry in document.getDocument("key").entries) {
            if (entry.value.isString && entry.value.asString().value in listOf("hashed")) {
                return true
            }
        }

        return false
    }

    private fun isTextIndex(document: BsonDocument): Boolean {
        return document.containsKey("textIndexVersion")
    }

    private fun createGeoSpatialIndex(document: BsonDocument, partialFilterExpression: BsonDocument?): Index {
        var index: Index? = null

        for (entry in document.getDocument("key").entries) {
            index = when (entry.value.asString().value) {
                "2d" -> TwoDIndex(document.getString("name").value, entry.key, partialFilterExpression)
                "2dsphere" -> TwoDSphereIndex(document.getString("name").value, entry.key, partialFilterExpression)
                else -> null
            }

            if (index != null) break
        }

        return index!!
    }

    /*
        {
          "v" : 2,
          "key" : {
            "coordinates" : "2d"
          },
          "name" : "coordinates_2d",
          "ns" : "digitalvault_integration.users"
        }

        {
          "v" : 2,
          "key" : {
            "loc" : "2dsphere"
          },
          "name" : "loc_2dsphere",
          "ns" : "digitalvault_integration.users",
          "2dsphereIndexVersion" : 3
        }
     */
    private fun isGeoSpatialIndex(document: BsonDocument): Boolean {
        for (entry in document.getDocument("key").entries) {
            if (entry.value.isString && entry.value.asString().value in listOf("2d", "2dsphere")) {
                return true
            }
        }

        return false
    }

    /*
        {
          "v" : 2,
          "key" : {
            "_fts" : "text",
            "_ftsx" : 1
          },
          "name" : "name_text",
          "ns" : "digitalvault_integration.users",
          "weights" : {
            "name" : 1
          },
          "default_language" : "english",
          "language_override" : "language",
          "textIndexVersion" : 3
        }
     */
    private fun createTextIndex(document: BsonDocument, partialFilterExpression: BsonDocument?): Index {
        return TextIndex(
            document.getString("name").value,
            document.getDocument("weights").map {
                TextField(listOf(it.key), it.value.asInt32().value)
            }, partialFilterExpression)
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

            // Find first document using the index and run the explain
            val result = db.runCommand(explainCommand, BsonDocument::class.java)
            return QueryPlan(result)
        }

        return null
    }
}