package com.mconsulting.indexrecommender

import org.bson.BsonArray
import org.bson.BsonDateTime
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.json.JsonWriterSettings
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class HelpersTest {

    @Test
    fun translateQueryToProjection() {
        val projectionDocument = BsonDocument()
            .append("games.id", BsonString("1"))
            .append("id", BsonInt32(2))

        val embeddedProjectionDocument = BsonDocument()
            .append("games", BsonDocument()
                .append("id", BsonString("1")))
            .append("id", BsonInt32(2))

        val deepEmbeddedProjectionDocument = BsonDocument()
            .append("games", BsonDocument()
                .append("id", BsonString("1"))
                .append("name", BsonString("name"))
                .append("obj", BsonDocument()
                    .append("title", BsonInt32(100))))
            .append("id", BsonInt32(2))

        val deepMixedEmbeddedProjectionDocument = BsonDocument()
            .append("games", BsonDocument()
                .append("id", BsonString("1"))
                .append("name.address", BsonString("name"))
                .append("obj", BsonDocument()
                    .append("title", BsonInt32(100))))
            .append("id", BsonInt32(2))

        val projection = generateProjection(projectionDocument)
        val embeddedProjection = generateProjection(embeddedProjectionDocument)
        val deepEmbeddedProjection = generateProjection(deepEmbeddedProjectionDocument)
        val deepMixedEmbeddedProjection = generateProjection(deepMixedEmbeddedProjectionDocument)

        // Assertions
        assertEquals(BsonDocument()
            .append("games.id", BsonInt32(1))
            .append("id", BsonInt32(1)), projection)

        assertEquals(BsonDocument()
            .append("games.id", BsonInt32(1))
            .append("id", BsonInt32(1)), embeddedProjection)

        assertEquals(BsonDocument()
            .append("games.id", BsonInt32(1))
            .append("games.name", BsonInt32(1))
            .append("games.obj.title", BsonInt32(1))
            .append("id", BsonInt32(1)), deepEmbeddedProjection)

        assertEquals(BsonDocument()
            .append("games.id", BsonInt32(1))
            .append("games.name.address", BsonInt32(1))
            .append("games.obj.title", BsonInt32(1))
            .append("id", BsonInt32(1)), deepMixedEmbeddedProjection)
    }

    @Test
    fun containsArrayTest() {
        val result = containsArray(BsonDocument()
            .append("game", BsonDocument()
                .append("ids", BsonArray(mutableListOf(
                    BsonDocument().append("id", BsonInt32(1))
                )))))

        val result2 = containsArray(BsonDocument()
            .append("game", BsonDocument()
                .append("ids", BsonArray(mutableListOf(
                    BsonInt32(1), BsonInt32(2)
                )))))

        val result3 = containsArray(BsonDocument()
            .append("game", BsonDocument()
                .append("ids", BsonArray(mutableListOf(
                    BsonInt32(1), BsonInt32(2)
                ))))
            .append("_id", BsonInt32(100))
            .append("peter", BsonDocument()
                .append("ole", BsonArray(mutableListOf(
                    BsonDocument().append("tu", BsonString("hello"))
                )))))

        val result4 = containsArray(BsonDocument()
            .append("game", BsonDocument()
                .append("ids", BsonInt32(100)))
            .append("_id", BsonInt32(100)))

        assertTrue(result)
        assertTrue(result2)
        assertTrue(result3)
        assertFalse(result4)
    }

    @Test
    fun testCommandToBsonDocument() {
        val document = commandToBsonDocument("""
            { a: ISODate("2012-12-19T06:01:17.171Z") }
        """.trimIndent())

//        println(document.toJson(JsonWriterSettings(true)))
        assertEquals(BsonDocument()
            .append("a", BsonDateTime(
                isoDateTimeStringToDateTime("2012-12-19T06:01:17.171Z").millis
            )), document)
    }
}