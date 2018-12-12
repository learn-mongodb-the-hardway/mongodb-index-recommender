package com.mconsulting.mschema.cli.output

import com.mconsulting.indexrecommender.CollectionIndexResults
import com.mconsulting.indexrecommender.CollectionStats
import com.mconsulting.indexrecommender.DbIndexResult
import com.mconsulting.indexrecommender.Namespace
import com.mconsulting.indexrecommender.indexes.CompoundIndex
import com.mconsulting.indexrecommender.indexes.CompoundTextIndex
import com.mconsulting.indexrecommender.indexes.Field
import com.mconsulting.indexrecommender.indexes.HashedIndex
import com.mconsulting.indexrecommender.indexes.Index
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.indexes.MultikeyIndex
import com.mconsulting.indexrecommender.indexes.SingleFieldIndex
import com.mconsulting.indexrecommender.indexes.TTLIndex
import com.mconsulting.indexrecommender.indexes.TextField
import com.mconsulting.indexrecommender.indexes.TextIndex
import com.mconsulting.indexrecommender.indexes.TwoDIndex
import com.mconsulting.indexrecommender.indexes.TwoDSphereIndex
import com.mconsulting.mschema.cli.IndentationWriter
import org.bson.BsonDocument
import org.bson.BsonString
import org.junit.jupiter.api.Test
import java.io.StringWriter
import kotlin.test.assertTrue

class JsFormatterTest {
    @Test
    fun shouldGenerateSingleFieldIndex() {
        val output = generateJs(listOf(
            SingleFieldIndex("name_1", Field("name", IndexDirection.ASCENDING))
        ))

        assertTrue(output.contains("db.users.createIndex({ \"name\" : 1 }, { \"name\" : \"name_1\", \"unique\" : false, \"sparse\" : false, \"background\" : true });"))
    }

    @Test
    fun shouldGenerateCompoundFieldIndex() {
        val output = generateJs(listOf(
            CompoundIndex("name_1_age_-1_city_1", listOf(
                Field("name", IndexDirection.ASCENDING),
                Field("age", IndexDirection.DESCENDING),
                Field("city", IndexDirection.UNKNOWN)
            ))
        ))

        assertTrue(output.contains("db.users.createIndex({ \"name\" : 1, \"age\" : -1, \"city\" : 1 }, { \"name\" : \"name_1_age_-1_city_1\", \"unique\" : false, \"sparse\" : false, \"background\" : true });"))
    }

    @Test
    fun shouldGenerateCompoundTextIndex() {
        val output = generateJs(listOf(
            CompoundTextIndex("name_1_age_-1_city_1", listOf(
                Field("name", IndexDirection.ASCENDING),
                Field("age", IndexDirection.DESCENDING)
            ), listOf(
                TextField(listOf("city"), 1)
            ))
        ))

        assertTrue(output.contains("db.users.createIndex({ \"name\" : 1, \"age\" : -1, \"city\" : \"text\" }, { \"name\" : \"name_1_age_-1_city_1\", \"unique\" : false, \"sparse\" : false, \"background\" : true, \"weights\" : { \"city\" : 1 } });"))
    }

    @Test
    fun shouldGenerateMultikeyIndex() {
        val output = generateJs(listOf(
            MultikeyIndex("name_1_age_-1_city_1", listOf(
                Field("name", IndexDirection.ASCENDING),
                Field("age", IndexDirection.DESCENDING)
            ), true, true, BsonDocument().append("a", BsonString("peter")))
        ))

        assertTrue(output.contains("db.users.createIndex({ \"name\" : 1, \"age\" : -1 }, { \"name\" : \"name_1_age_-1_city_1\", \"unique\" : true, \"sparse\" : true, \"background\" : true, \"partialFilterExpression\" : { \"a\" : \"peter\" } });"))
    }

    @Test
    fun shouldGenerateTextIndex() {
        val output = generateJs(listOf(
            TextIndex("name_1_age_-1_city_1", listOf(
                TextField(listOf("name"), 2)
            ), BsonDocument().append("a", BsonString("peter")))
        ))

        assertTrue(output.contains("db.users.createIndex({ \"name\" : \"text\" }, { \"name\" : \"name_1_age_-1_city_1\", \"unique\" : false, \"sparse\" : false, \"background\" : true, \"partialFilterExpression\" : { \"a\" : \"peter\" }, \"weights\" : { \"name\" : 2 } });"))
    }

    @Test
    fun shouldGenerateTwoDSphereIndex() {
        val output = generateJs(listOf(
            TwoDSphereIndex("name_1_age_-1_city_1", "loc", BsonDocument().append("a", BsonString("peter")))
        ))

        assertTrue(output.contains("db.users.createIndex({ \"loc\": \"2dsphere\" }, { \"name\" : \"name_1_age_-1_city_1\", \"unique\" : false, \"sparse\" : false, \"background\" : true, \"partialFilterExpression\" : { \"a\" : \"peter\" } });"))
    }

    @Test
    fun shouldGenerateTwoDIndex() {
        val output = generateJs(listOf(
            TwoDIndex("name_1_age_-1_city_1", "loc", BsonDocument().append("a", BsonString("peter")))
        ))

        assertTrue(output.contains("db.users.createIndex({ \"loc\": \"2d\" }, { \"name\" : \"name_1_age_-1_city_1\", \"unique\" : false, \"sparse\" : false, \"background\" : true, \"partialFilterExpression\" : { \"a\" : \"peter\" } });"))
    }

    @Test
    fun shouldGenerateHashedIndex() {
        val output = generateJs(listOf(
            HashedIndex("_id_1", "_id", BsonDocument().append("a", BsonString("peter")))
        ))

        assertTrue(output.contains("db.users.createIndex({ \"_id\": \"hashed\" }, { \"name\" : \"_id_1\", \"unique\" : false, \"sparse\" : false, \"background\" : true, \"partialFilterExpression\" : { \"a\" : \"peter\" } });"))
    }

    @Test
    fun shouldGenerateTTLIndex() {
        val output = generateJs(listOf(
            TTLIndex("created_1", Field("created", IndexDirection.UNKNOWN), 10000, BsonDocument().append("a", BsonString("peter")))
        ))

        assertTrue(output.contains("db.users.createIndex({ \"created\" : 1 }, { \"name\" : \"created_1\", \"unique\" : false, \"sparse\" : false, \"background\" : true, \"partialFilterExpression\" : { \"a\" : \"peter\" }, \"expireAfterSeconds\" : 10000 });"))
    }

    private fun generateJs(indexes: List<Index> = listOf()) : String {
        val formatter = JsFormatter()
        // Create the writer
        val writer = IndentationWriter(StringWriter())

        // Render the js
        formatter.render(DbIndexResult(Namespace("quickstart", "users"), listOf(
            CollectionIndexResults(Namespace("quickstart", "users"), indexes, listOf(), CollectionStats(BsonDocument()))
        )), writer)

        return writer.toString()
    }
}