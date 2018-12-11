package com.mconsulting.indexrecommender

import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.junit.jupiter.api.Test
import java.io.StringReader
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

        val projection = generateProjection(Parser().parse(StringReader(projectionDocument.toJson())) as JsonObject)
        val embeddedProjection = generateProjection(Parser().parse(StringReader(embeddedProjectionDocument.toJson())) as JsonObject)
        val deepEmbeddedProjection = generateProjection(Parser().parse(StringReader(deepEmbeddedProjectionDocument.toJson())) as JsonObject)
        val deepMixedEmbeddedProjection = generateProjection(Parser().parse(StringReader(deepMixedEmbeddedProjectionDocument.toJson())) as JsonObject)

        // Assertions
        assertEquals(JsonObject(mapOf(
            "games.id" to 1,
            "id" to 1
        )), projection)

        assertEquals(JsonObject(mapOf(
            "games.id" to 1,
            "id" to 1
        )), embeddedProjection)

        assertEquals(JsonObject(mapOf(
            "games.id" to 1,
            "games.name" to 1,
            "games.obj.title" to 1,
            "id" to 1
        )), deepEmbeddedProjection)

        assertEquals(JsonObject(mapOf(
            "games.id" to 1,
            "games.name.address" to 1,
            "games.obj.title" to 1,
            "id" to 1
        )), deepMixedEmbeddedProjection)
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
        val document = commandToJsonObject("""
            {
                a: ISODate("2012-12-19T06:01:17.171Z"),
                b: ObjectId("5be98d693c871d68db7a70ad"),
                c: NumberLong("2090845886852"),
                d: NumberInt("10000"),
                e: NumberDecimal("10.10"),
                e1: NumberDecimal("12121"),
                f: BinData(2,"ZGFhZGE="),
                j: Timestamp(0, 0),
                k: undefined,
                l: Boolean("true"),
                l1: Boolean(true),
                s: /a/i,
                o: MinKey,
                p: MaxKey,
                u: UUID("b8a51588-fc4d-4bfc-89e4-903ba7ffadc1")
            }
        """.trimIndent())

        assertEquals(Parser().parse(StringReader("""
            {
              "a": {
                "${'$'}date": "2012-12-19T06:01:17.171Z"
              },
              "b": {
                "${'$'}oid": "5be98d693c871d68db7a70ad"
              },
              "c": {
                "${'$'}numberLong": "2090845886852"
              },
              "d": 10000,
              "e": {
                "${'$'}numberDecimal": "10.10"
              },
              "e1": {
                "${'$'}numberDecimal": "12121"
              },
              "f": {
                "${'$'}binary": "ZGFhZGE=",
                "${'$'}type": "2"
              },
              "j": {
                "${'$'}timestamp": {
                  "t": 0,
                  "i": 0
                }
              },
              "k": {
                "${'$'}undefined": true
              },
              "l": true,
              "l1": true,
              "s": {
                "${'$'}regex": "a",
                "${'$'}options": "i"
              },
              "o": {
                "${'$'}minKey": 1
              },
              "p": {
                "${'$'}maxKey": 1
              },
              "u": {
                "${'$'}binary": "YjhhNTE1ODgtZmM0ZC00YmZjLTg5ZTQtOTAzYmE3ZmZhZGMx",
                "${'$'}type": "04"
              }
            }
        """.trimIndent())), document)
    }
}