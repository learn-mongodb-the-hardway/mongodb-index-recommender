package com.mconsulting.indexrecommender

import org.bson.BsonDocument
import java.io.InputStreamReader

fun readResourceAsString(resource: String) =
    InputStreamReader(IndexRecommendationEngineTest::class.java.classLoader.getResourceAsStream(resource)).readText()

fun readJsonAsBsonDocument(resource: String) = BsonDocument.parse(readResourceAsString(resource))