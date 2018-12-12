package com.mconsulting.indexrecommender.indexes

import com.mconsulting.indexrecommender.readJsonAsBsonDocument
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.IndexOptions
import com.vdurmont.semver4j.Semver
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class IndexTests {
    private fun createIndex(doc: BsonDocument) : Index {
        return IndexParser(client, IndexParserOptions(true)).createIndex(doc)
    }

    @Test
    fun _idIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/_id_index.json")) as IdIndex
        assertEquals("_id_", index.name)
        assertEquals(true, index.unique)
    }

    @Test
    fun singleFieldIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/single_field_index.json")) as SingleFieldIndex
        assertEquals("name_1", index.name)
        assertEquals(Field("name", IndexDirection.ASCENDING), index.field)
        assertEquals(false, index.sparse)
        assertEquals(false, index.unique)
    }

    @Test
    fun compoundIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/compound_field_index.json")) as CompoundIndex
        assertEquals("name_1_text_-1", index.name)
        assertEquals(listOf(
            Field("name", IndexDirection.ASCENDING),
            Field("text", IndexDirection.DESCENDING)
        ), index.fields)
        assertEquals(false, index.sparse)
        assertEquals(false, index.unique)
    }

    @Test
    fun hashedIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/hashed_index.json")) as HashedIndex
        assertEquals("_id", index.field)
        assertEquals("_id_hashed", index.name)
        assertEquals(false, index.sparse)
        assertEquals(false, index.unique)
    }

    @Test
    fun partialIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/partial_index.json")) as SingleFieldIndex
        assertEquals("game_1", index.name)
        assertEquals("game", index.field.name)
        assertEquals(IndexDirection.ASCENDING, index.field.direction)
        assertEquals(false, index.sparse)
        assertEquals(false, index.unique)
        assertEquals(BsonDocument().append("rating", BsonDocument().append("\$gt", BsonInt32(5))), index.partialFilterExpression)
    }

    @Test
    fun sparseIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/sparse_index.json")) as SingleFieldIndex
        assertEquals("description_1", index.name)
        assertEquals("description", index.field.name)
        assertEquals(IndexDirection.ASCENDING, index.field.direction)
        assertEquals(true, index.sparse)
        assertEquals(false, index.unique)
    }

    @Test
    fun textIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/text_index.json")) as TextIndex
        assertEquals("name_text", index.name)
        assertEquals(listOf(
            TextField(listOf("name"), 1)
        ), index.fields)
        assertEquals(false, index.sparse)
        assertEquals(false, index.unique)
        assertNull(index.partialFilterExpression)
    }

    @Test
    fun compoundTextIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/text_compound_index.json")) as CompoundTextIndex
        assertEquals("a_1_c_1_b_text_d_text", index.name)
        assertEquals(listOf(
            TextField(listOf("b"), 1),
            TextField(listOf("d"), 1)
        ), index.fields)
        assertEquals(listOf(
            Field("a", IndexDirection.ASCENDING),
            Field("c", IndexDirection.ASCENDING),
            Field("_fts", IndexDirection.UNKNOWN),
            Field("_ftsx", IndexDirection.ASCENDING)
        ), index.compoundFields)
        assertEquals(false, index.sparse)
        assertEquals(false, index.unique)
        assertNull(index.partialFilterExpression)
    }

    @Test
    fun compoundGlobalTextIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/text_global_index.json")) as CompoundTextIndex
        assertEquals("a_1_\$**_text", index.name)
        assertEquals(listOf(
            TextField(listOf("$**"), 1)
        ), index.fields)
        assertEquals(listOf(
            Field("a", IndexDirection.ASCENDING),
            Field("_fts", IndexDirection.UNKNOWN),
            Field("_ftsx", IndexDirection.ASCENDING)
        ), index.compoundFields)
        assertEquals(false, index.sparse)
        assertEquals(false, index.unique)
        assertNull(index.partialFilterExpression)
    }

    @Test
    fun twoDIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/two_d_index.json")) as TwoDIndex
        assertEquals("coordinates_2d", index.name)
        assertEquals("coordinates", index.key)
        assertEquals(false, index.sparse)
        assertEquals(false, index.unique)
        assertNull(index.partialFilterExpression)
    }

    @Test
    fun twoDSphereIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/two_d_sphere_index.json")) as TwoDSphereIndex
        assertEquals("loc_2dsphere", index.name)
        assertEquals("loc", index.key)
        assertEquals(false, index.sparse)
        assertEquals(false, index.unique)
        assertNull(index.partialFilterExpression)
    }

    @Test
    fun uniqueIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/unique_index.json")) as SingleFieldIndex
        assertEquals("title_1", index.name)
        assertEquals(Field("title", IndexDirection.ASCENDING), index.field)
        assertEquals(false, index.sparse)
        assertEquals(true, index.unique)
        assertNull(index.partialFilterExpression)
    }

    @Test
    fun multiKeyIndex() {
        assumeTrue(mongoDbVersion(">=3.6"), "MongoDB version is [$mongodbVersion] which does not match >=3.6")

        val index = createIndex(readJsonAsBsonDocument("indexes/multikey_index.json")) as MultikeyIndex
        assertEquals("a.b_-1", index.name)
        assertEquals(listOf(
            Field("a.b", IndexDirection.DESCENDING)
        ), index.fields)
        assertEquals(false, index.sparse)
        assertEquals(false, index.unique)
    }

    private fun mongoDbVersion(version: String): Boolean {
        val semVer = Semver(mongodbVersion, Semver.SemverType.NPM)
        return semVer.satisfies(version)
    }

    @Test
    fun timeToLiveIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/ttl_index.json")) as TTLIndex
        assertEquals("lastModifiedDate_1", index.name)
        assertEquals(Field("lastModifiedDate", IndexDirection.ASCENDING), index.field)
        assertEquals(3600, index.expireAfterSeconds)
        assertEquals(false, index.sparse)
        assertEquals(false, index.unique)
    }

    companion object {
        lateinit var client: MongoClient
        lateinit var db: MongoDatabase
        lateinit var collection: MongoCollection<Document>
        lateinit var mongodbVersion: String

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            client = MongoClient(MongoClientURI("mongodb://localhost:27017"))
            db = client.getDatabase("mindex_recommendation_tests")
            collection = db.getCollection("index_tests")

            // Drop collection
            collection.drop()

            // Get the mongoodb version
            val commandResult = db.runCommand(BsonDocument().append("buildInfo", BsonInt32(1)))
            mongodbVersion = commandResult.getString("version")

            // Insert a test document
            collection.insertOne(Document(mapOf(
                "a" to listOf(Document(mapOf(
                    "b" to 1
                )))
            )))

            // Generate test indexes
            collection.createIndex(Document(mapOf(
                "a.b" to -1
            )), IndexOptions().background(false))
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }
}