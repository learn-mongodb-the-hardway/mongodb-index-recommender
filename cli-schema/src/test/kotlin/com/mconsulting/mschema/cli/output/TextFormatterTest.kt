package com.mconsulting.mschema.cli.output

import com.mconsulting.indexrecommender.CollectionIndexResults
import com.mconsulting.indexrecommender.CollectionStats
import com.mconsulting.indexrecommender.DbIndexResult
import com.mconsulting.indexrecommender.Namespace
import com.mconsulting.indexrecommender.indexes.Field
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.indexes.SingleFieldIndex
import com.mconsulting.mschema.cli.IndentationWriter
import com.mconsulting.mschema.cli.writeln
import org.bson.BsonDocument
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.StringWriter

class TextFormatterTest {

    @Test
    fun shouldPrintCollectionCount() {
        val formatter = TextFormatter()
        val writer = IndentationWriter(StringWriter())
        formatter.render(DbIndexResult(
            Namespace("quickstart", "users"),
            listOf(
                CollectionIndexResults(
                    Namespace("quickstart", "users"),
                    listOf(),
                    listOf(),
                    CollectionStats(generateBsonDocument(
                        count = 1000
                    ))
                )
            )
        ), writer)

        assertTrue(writer.toString().contains("number of documents <"))
    }

    @Test
    fun shouldPrintHighReadRatio() {
        val formatter = TextFormatter()
        val writer = IndentationWriter(StringWriter())
        formatter.render(DbIndexResult(
            Namespace("quickstart", "users"),
            listOf(
                CollectionIndexResults(
                    Namespace("quickstart", "users"),
                    listOf(),
                    listOf(),
                    CollectionStats(generateBsonDocument(
                        count = 1000000, finds = 100000, inserts = 2000
                    ))
                )
            )
        ), writer)

        assertTrue(writer.toString().contains("read/write ratio >"))
    }

    @Test
    fun shouldPrintHighWriteRatio() {
        val formatter = TextFormatter()
        val writer = IndentationWriter(StringWriter())
        formatter.render(DbIndexResult(
            Namespace("quickstart", "users"),
            listOf(
                CollectionIndexResults(
                    Namespace("quickstart", "users"),
                    listOf(),
                    listOf(),
                    CollectionStats(generateBsonDocument(
                        count = 1000000, finds = 100, inserts = 2000, update = 3000, remove = 2000
                    ))
                )
            )
        ), writer)

        assertTrue(writer.toString().contains("read/write ratio <"))
    }

    @Test
    fun shouldPrintHighWriteRatioAndWarning() {
        val formatter = TextFormatter()
        val writer = IndentationWriter(StringWriter())
        formatter.render(DbIndexResult(
            Namespace("quickstart", "users"),
            listOf(
                CollectionIndexResults(
                    Namespace("quickstart", "users"),
                    listOf(),
                    listOf(),
                    CollectionStats(generateBsonDocument(
                        count = 1000000, finds = 100, inserts = 200000, update = 1000, remove = 1000
                    ))
                )
            )
        ), writer)

        assertTrue(writer.toString().contains("the insert/update ratio is"))
    }

    @Test
    fun shouldPrintHighIndexCountWarning() {
        val formatter = TextFormatter()
        val writer = IndentationWriter(StringWriter())
        formatter.render(DbIndexResult(
            Namespace("quickstart", "users"),
            listOf(
                CollectionIndexResults(
                    Namespace("quickstart", "users"),
                    listOf(
                        SingleFieldIndex("a", Field("a", IndexDirection.ASCENDING)),
                        SingleFieldIndex("a", Field("a", IndexDirection.ASCENDING)),
                        SingleFieldIndex("a", Field("a", IndexDirection.ASCENDING)),
                        SingleFieldIndex("a", Field("a", IndexDirection.ASCENDING)),
                        SingleFieldIndex("a", Field("a", IndexDirection.ASCENDING)),
                        SingleFieldIndex("a", Field("a", IndexDirection.ASCENDING)),
                        SingleFieldIndex("a", Field("a", IndexDirection.ASCENDING)),
                        SingleFieldIndex("a", Field("a", IndexDirection.ASCENDING)),
                        SingleFieldIndex("a", Field("a", IndexDirection.ASCENDING)),
                        SingleFieldIndex("a", Field("a", IndexDirection.ASCENDING)),
                        SingleFieldIndex("a", Field("a", IndexDirection.ASCENDING)),
                        SingleFieldIndex("a", Field("a", IndexDirection.ASCENDING)),
                        SingleFieldIndex("a", Field("a", IndexDirection.ASCENDING)),
                        SingleFieldIndex("a", Field("a", IndexDirection.ASCENDING)),
                        SingleFieldIndex("a", Field("a", IndexDirection.ASCENDING)),
                        SingleFieldIndex("a", Field("a", IndexDirection.ASCENDING))
                    ),
                    listOf(),
                    CollectionStats(generateBsonDocument(
                        count = 1000000
                    ))
                )
            )
        ), writer)

//        println(writer.toString())
        assertTrue(writer.toString().contains("index count >"))
    }

    fun generateBsonDocument(
        count: Int = 0, finds: Int = 0, inserts: Int = 0, remove: Int = 0, update: Int = 0
    ): BsonDocument {
        val jsonTemplate = """{
              "ns" : "mindex_recommendation_tests.t",
              "count" : $count,
              "wiredTiger" : {
                "cursor" : {
                  "bulk-loaded cursor-insert calls" : 0,
                  "create calls" : 1,
                  "cursor operation restarted" : 0,
                  "cursor-insert key and value bytes inserted" : 183,
                  "cursor-remove key bytes removed" : 0,
                  "cursor-update value bytes updated" : 0,
                  "cursors cached on close" : 0,
                  "cursors reused from cache" : 19,
                  "insert calls" : $inserts,
                  "modify calls" : 0,
                  "next calls" : $finds,
                  "prev calls" : 1,
                  "remove calls" : $remove,
                  "reserve calls" : 0,
                  "reset calls" : 41,
                  "search calls" : 0,
                  "search near calls" : 0,
                  "truncate calls" : 0,
                  "update calls" : $update
                }
              },
              "nindexes" : 2
            }""".trimIndent()

        return BsonDocument.parse(jsonTemplate)
    }
}