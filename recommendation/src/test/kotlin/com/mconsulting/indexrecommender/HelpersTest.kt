package com.mconsulting.indexrecommender

import org.bson.BsonArray
import org.bson.BsonBinary
import org.bson.BsonBinarySubType
import org.bson.BsonBoolean
import org.bson.BsonDateTime
import org.bson.BsonDecimal128
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonMaxKey
import org.bson.BsonMinKey
import org.bson.BsonObjectId
import org.bson.BsonRegularExpression
import org.bson.BsonString
import org.bson.BsonTimestamp
import org.bson.BsonUndefined
import org.bson.json.JsonWriterSettings
import org.bson.types.Decimal128
import org.bson.types.ObjectId
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class HelpersTest {

//    @Test
//    fun translateQueryToProjection() {
//        val projectionDocument = BsonDocument()
//            .append("games.id", BsonString("1"))
//            .append("id", BsonInt32(2))
//
//        val embeddedProjectionDocument = BsonDocument()
//            .append("games", BsonDocument()
//                .append("id", BsonString("1")))
//            .append("id", BsonInt32(2))
//
//        val deepEmbeddedProjectionDocument = BsonDocument()
//            .append("games", BsonDocument()
//                .append("id", BsonString("1"))
//                .append("name", BsonString("name"))
//                .append("obj", BsonDocument()
//                    .append("title", BsonInt32(100))))
//            .append("id", BsonInt32(2))
//
//        val deepMixedEmbeddedProjectionDocument = BsonDocument()
//            .append("games", BsonDocument()
//                .append("id", BsonString("1"))
//                .append("name.address", BsonString("name"))
//                .append("obj", BsonDocument()
//                    .append("title", BsonInt32(100))))
//            .append("id", BsonInt32(2))
//
//        val projection = generateProjection(projectionDocument)
//        val embeddedProjection = generateProjection(embeddedProjectionDocument)
//        val deepEmbeddedProjection = generateProjection(deepEmbeddedProjectionDocument)
//        val deepMixedEmbeddedProjection = generateProjection(deepMixedEmbeddedProjectionDocument)
//
//        // Assertions
//        assertEquals(BsonDocument()
//            .append("games.id", BsonInt32(1))
//            .append("id", BsonInt32(1)), projection)
//
//        assertEquals(BsonDocument()
//            .append("games.id", BsonInt32(1))
//            .append("id", BsonInt32(1)), embeddedProjection)
//
//        assertEquals(BsonDocument()
//            .append("games.id", BsonInt32(1))
//            .append("games.name", BsonInt32(1))
//            .append("games.obj.title", BsonInt32(1))
//            .append("id", BsonInt32(1)), deepEmbeddedProjection)
//
//        assertEquals(BsonDocument()
//            .append("games.id", BsonInt32(1))
//            .append("games.name.address", BsonInt32(1))
//            .append("games.obj.title", BsonInt32(1))
//            .append("id", BsonInt32(1)), deepMixedEmbeddedProjection)
//    }
//
//    @Test
//    fun containsArrayTest() {
//        val result = containsArray(BsonDocument()
//            .append("game", BsonDocument()
//                .append("ids", BsonArray(mutableListOf(
//                    BsonDocument().append("id", BsonInt32(1))
//                )))))
//
//        val result2 = containsArray(BsonDocument()
//            .append("game", BsonDocument()
//                .append("ids", BsonArray(mutableListOf(
//                    BsonInt32(1), BsonInt32(2)
//                )))))
//
//        val result3 = containsArray(BsonDocument()
//            .append("game", BsonDocument()
//                .append("ids", BsonArray(mutableListOf(
//                    BsonInt32(1), BsonInt32(2)
//                ))))
//            .append("_id", BsonInt32(100))
//            .append("peter", BsonDocument()
//                .append("ole", BsonArray(mutableListOf(
//                    BsonDocument().append("tu", BsonString("hello"))
//                )))))
//
//        val result4 = containsArray(BsonDocument()
//            .append("game", BsonDocument()
//                .append("ids", BsonInt32(100)))
//            .append("_id", BsonInt32(100)))
//
//        assertTrue(result)
//        assertTrue(result2)
//        assertTrue(result3)
//        assertFalse(result4)
//    }
//
//    @Test
//    fun testCommandToBsonDocument() {
//        val document = commandToBsonDocument("""
//            {
//                a: ISODate("2012-12-19T06:01:17.171Z"),
//                b: ObjectId("5be98d693c871d68db7a70ad"),
//                c: NumberLong("2090845886852"),
//                d: NumberInt("10000"),
//                e: NumberDecimal("10.10"),
//                e1: NumberDecimal("12121"),
//                f: BinData(2,"ZGFhZGE="),
//                j: Timestamp(0, 0),
//                k: undefined,
//                l: Boolean("true"),
//                l1: Boolean(true),
//                o: MinKey,
//                p: MaxKey,
//                s: /a/i,
//                u: UUID("b8a51588-fc4d-4bfc-89e4-903ba7ffadc1")
//            }
//        """.trimIndent())
//
//        assertEquals(BsonDocument()
//            .append("a", BsonDateTime(isoDateTimeStringToDateTime("2012-12-19T06:01:17.171Z").millis))
//            .append("b", BsonObjectId(ObjectId("5be98d693c871d68db7a70ad")))
//            .append("c", BsonInt64(2090845886852))
//            .append("d", BsonInt32(10000))
//            .append("e", BsonDecimal128(Decimal128(BigDecimal("10.10"))))
//            .append("e1", BsonDecimal128(Decimal128(BigDecimal("12121"))))
//            .append("f", BsonBinary(BsonBinarySubType.OLD_BINARY, "daada".toByteArray()))
//            .append("j", BsonTimestamp(0, 0))
//            .append("k", BsonUndefined())
//            .append("l", BsonBoolean(true))
//            .append("l1", BsonBoolean(true))
//            .append("o", BsonMinKey())
//            .append("p", BsonMaxKey())
//            .append("s", BsonRegularExpression("a", "i"))
//            .append("u", BsonBinary(BsonBinarySubType.UUID_STANDARD, "b8a51588-fc4d-4bfc-89e4-903ba7ffadc1".toByteArray())), document)
//    }
}