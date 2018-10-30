package com.mconsulting.indexrecommender.indexes

import com.mconsulting.indexrecommender.readJsonAsBsonDocument
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.BsonDocument
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class IndexTests {

    private fun createIndex(doc: BsonDocument) : Index {
        val parser = IndexParser(client, IndexParserOptions(true))
        return parser.createIndex(doc)
    }

    @Test
    fun _idIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/_id_index.json"))
        assertTrue(index is IdIndex)
        assertEquals(true, index.unique)
    }

    @Test
    fun singleFieldIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/single_field_index.json"))
        assertTrue(index is SingleFieldIndex)
        assertEquals(Field("name", IndexDirection.ASCENDING), (index as SingleFieldIndex).field)
        assertEquals(false, index.sparse)
        assertEquals(false, index.unique)
    }

    @Test
    fun compoundIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/compound_field_index.json"))
        assertTrue(index is CompoundIndex)
        assertEquals(listOf(
            Field("name", IndexDirection.ASCENDING),
            Field("text", IndexDirection.DESCENDING)
        ), (index as CompoundIndex).fields)
        assertEquals(false, index.sparse)
        assertEquals(false, index.unique)
    }

    @Test
    fun hashedIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/hashed_index.json"))
        assertTrue(index is HashedIndex)
        assertEquals("_id", (index as HashedIndex).field)
        assertEquals(false, index.sparse)
        assertEquals(false, index.unique)
    }

    @Test
    fun multiKeyIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/multikey_index.json"))
        assertTrue(index is MultikeyIndex)
//        assertEquals(listOf(
//            Field("name", IndexDirection.ASCENDING),
//            Field("text", IndexDirection.DESCENDING)
//        ), (index as CompoundIndex).fields)
//        assertEquals(false, index.sparse)
//        assertEquals(false, index.unique)
    }

    @Test
    fun partialIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/partial_index.json"))
        assertTrue(index is PartialIndex)
//        assertEquals(listOf(
//            Field("name", IndexDirection.ASCENDING),
//            Field("text", IndexDirection.DESCENDING)
//        ), (index as CompoundIndex).fields)
//        assertEquals(false, index.sparse)
//        assertEquals(false, index.unique)
    }

    @Test
    fun sparseIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/sparse_index.json"))
        assertTrue(index is SingleFieldIndex)
//        assertEquals(listOf(
//            Field("name", IndexDirection.ASCENDING),
//            Field("text", IndexDirection.DESCENDING)
//        ), (index as CompoundIndex).fields)
        assertEquals(true, index.sparse)
//        assertEquals(false, index.unique)
    }

    @Test
    fun textIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/text_index.json"))
        assertTrue(index is TextIndex)
//        assertEquals(listOf(
//            Field("name", IndexDirection.ASCENDING),
//            Field("text", IndexDirection.DESCENDING)
//        ), (index as CompoundIndex).fields)
//        assertEquals(true, index.sparse)
//        assertEquals(false, index.unique)
    }

    @Test
    fun twoDIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/two_d_index.json"))
        assertTrue(index is TwoDIndex)
//        assertEquals(listOf(
//            Field("name", IndexDirection.ASCENDING),
//            Field("text", IndexDirection.DESCENDING)
//        ), (index as CompoundIndex).fields)
//        assertEquals(true, index.sparse)
//        assertEquals(false, index.unique)
    }

    @Test
    fun twoDSphereIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/two_d_sphere_index.json"))
        assertTrue(index is TwoDSphereIndex)
//        assertEquals(listOf(
//            Field("name", IndexDirection.ASCENDING),
//            Field("text", IndexDirection.DESCENDING)
//        ), (index as CompoundIndex).fields)
//        assertEquals(true, index.sparse)
//        assertEquals(false, index.unique)
    }

    @Test
    fun uniqueIndex() {
        val index = createIndex(readJsonAsBsonDocument("indexes/unique_index.json"))
        assertTrue(index is SingleFieldIndex)
//        assertEquals(listOf(
//            Field("name", IndexDirection.ASCENDING),
//            Field("text", IndexDirection.DESCENDING)
//        ), (index as CompoundIndex).fields)
//        assertEquals(true, index.sparse)
        assertEquals(true, index.unique)
    }

    companion object {
        lateinit var client: MongoClient
        lateinit var db: MongoDatabase
        lateinit var collection: MongoCollection<Document>

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            client = MongoClient(MongoClientURI("mongodb://localhost:27017"))
            db = client.getDatabase("mindex_recommendation_tests")
            collection = db.getCollection("index_tests")

            // Drop collection
            collection.drop()

            // Insert a test document
            collection.insertOne(Document(mapOf(
                "a" to listOf(Document(mapOf(
                    "b" to 1
                )))
            )))

//            // Generate test indexes
//            collection.createIndex(Document(mapOf(
//
//            )))
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }
}