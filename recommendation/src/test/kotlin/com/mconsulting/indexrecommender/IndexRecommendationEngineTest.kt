package com.mconsulting.indexrecommender

import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import com.mconsulting.indexrecommender.indexes.Field
import com.mconsulting.indexrecommender.indexes.Index
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.indexes.SingleFieldIndex
import com.mconsulting.indexrecommender.indexes.TwoDSphereIndex
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoDatabase
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.io.StringReader

class IndexRecommendationEngineTest {

    private fun createEngine(client: MongoClient, dbName:String, collectionName:String) : IndexRecommendationEngine {
        val namespace = Namespace(dbName, collectionName)
        val db = Db(client, namespace)
        val collection = Collection(client, namespace, db)
        return IndexRecommendationEngine(client, collection)
    }

    private fun recommend(client: MongoClient, query: String) : List<Index> {
        val engine = createEngine(Companion.client, "test", "jstests_geo_poly_edge")
        // Add the operation
        val operation = createOperation(Parser().parse(StringReader(query)) as JsonObject)!!
        engine.process(operation)
        return engine.recommend()
    }

    @Test
    fun handleGeoOperationsInQuery() {
        val query = """{"op":"query","ns":"test.jstests_geo_poly_edge","query":{"find":"jstests_geo_poly_edge","filter":{"loc":{"${'$'}within":{"${'$'}polygon":[[10.0,10.0],[10.0,10.0],[10.0,-10.0]]}}}},"keysExamined":2,"docsExamined":2,"cursorExhausted":true,"numYield":0,"locks":{"Global":{"acquireCount":{"r":{"${'$'}numberLong":"2"}}},"Database":{"acquireCount":{"r":{"${'$'}numberLong":"1"}}},"Collection":{"acquireCount":{"r":{"${'$'}numberLong":"1"}}}},"nreturned":2,"responseLength":213,"protocol":"op_command","millis":0,"planSummary":"IXSCAN { loc: \"2d\" }","execStats":{"stage":"FETCH","filter":{"loc":{"${'$'}within":{"${'$'}polygon":[[10.0,10.0],[10.0,10.0],[10.0,-10.0]]}}},"nReturned":2,"executionTimeMillisEstimate":0,"works":3,"advanced":2,"needTime":0,"needYield":0,"saveState":0,"restoreState":0,"isEOF":1,"invalidates":0,"docsExamined":2,"alreadyHasObj":0,"inputStage":{"stage":"IXSCAN","nReturned":2,"executionTimeMillisEstimate":0,"works":3,"advanced":2,"needTime":0,"needYield":0,"saveState":0,"restoreState":0,"isEOF":1,"invalidates":0,"keyPattern":{"loc":"2d"},"indexName":"loc_2d","isMultiKey":false,"isUnique":false,"isSparse":false,"isPartial":false,"indexVersion":2,"direction":"forward","indexBounds":{"loc":["[BinData(128, 956A540000000000), BinData(128, 956A57FFFFFFFFFF)]","[BinData(128, 956B000000000000), BinData(128, 956BFFFFFFFFFFFF)]","[BinData(128, 956E000000000000), BinData(128, 956EFFFFFFFFFFFF)]","[BinData(128, 956F000000000000), BinData(128, 956FFFFFFFFFFFFF)]","[BinData(128, 957A000000000000), BinData(128, 957AFFFFFFFFFFFF)]","[BinData(128, 957B000000000000), BinData(128, 957BFFFFFFFFFFFF)]","[BinData(128, 957E000000000000), BinData(128, 957EFFFFFFFFFFFF)]","[BinData(128, 957F000000000000), BinData(128, 957FFFFFFFFFFFFF)]","[BinData(128, C02A000000000000), BinData(128, C02AFFFFFFFFFFFF)]","[BinData(128, C02B000000000000), BinData(128, C02BFFFFFFFFFFFF)]","[BinData(128, C02E000000000000), BinData(128, C02EFFFFFFFFFFFF)]","[BinData(128, C02F000000000000), BinData(128, C02FFFFFFFFFFFFF)]","[BinData(128, C03A000000000000), BinData(128, C03AFFFFFFFFFFFF)]","[BinData(128, C03B000000000000), BinData(128, C03BFFFFFFFFFFFF)]","[BinData(128, C03E000000000000), BinData(128, C03EFFFFFFFFFFFF)]","[BinData(128, C03F000000000000), BinData(128, C03F03FFFFFFFFFF)]"]},"keysExamined":2,"seeks":1,"dupsTested":0,"dupsDropped":0,"seenInvalidated":0}},"ts":{"${'$'}date":"2018-11-22T15:58:59.182Z"},"client":"127.0.0.1","appName":"MongoDB Shell","allUsers":[],"user":""}"""
        val indexes = recommend(client, query)

        assertEquals(1, indexes.size)
        assertEquals(TwoDSphereIndex(
            "loc_1", "loc"
        ), indexes[0])
    }

    @Test
    fun handleGeoORperationInQuery2() {
        val query = """{"op":"update","ns":"test.geo_polygon1","query":{"_id":0.0},"updateobj":{"_id":0.0,"loc":[1.0,1.0]},"keysExamined":0,"docsExamined":0,"nMatched":0,"nModified":0,"upsert":true,"keysInserted":1,"numYield":0,"locks":{"Global":{"acquireCount":{"r":{"${'$'}numberLong":"3"},"w":{"${'$'}numberLong":"3"}}},"Database":{"acquireCount":{"w":{"${'$'}numberLong":"2"},"W":{"${'$'}numberLong":"1"}}},"Collection":{"acquireCount":{"w":{"${'$'}numberLong":"2"}}}},"millis":21,"planSummary":"IDHACK","execStats":{"stage":"UPDATE","nReturned":0,"executionTimeMillisEstimate":0,"works":2,"advanced":0,"needTime":1,"needYield":0,"saveState":0,"restoreState":0,"isEOF":1,"invalidates":0,"nMatched":0,"nWouldModify":0,"nInvalidateSkips":0,"wouldInsert":true,"fastmodinsert":false,"inputStage":{"stage":"IDHACK","nReturned":0,"executionTimeMillisEstimate":0,"works":1,"advanced":0,"needTime":0,"needYield":0,"saveState":0,"restoreState":0,"isEOF":1,"invalidates":0,"keysExamined":0,"docsExamined":0}},"ts":{"${'$'}date":"2018-11-22T15:58:59.377Z"},"client":"127.0.0.1","appName":"MongoDB Shell","allUsers":[],"user":""}"""
        val indexes = recommend(client, query)

        assertEquals(1, indexes.size)
        assertEquals(SingleFieldIndex(
            "_id_1", Field("_id", IndexDirection.UNKNOWN)
        ), indexes[0])
    }

    companion object {
        lateinit var client: MongoClient
        lateinit var db: MongoDatabase

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            client = MongoClient(MongoClientURI("mongodb://localhost:27017"))
            db = client.getDatabase("mindex_recommendation_tests")
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }
}