package com.mconsulting.indexrecommender

import org.bson.BsonDocument
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.CodecRegistries
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import java.io.InputStreamReader

fun readResourceAsString(resource: String) =
    InputStreamReader(QueryRecommendationTest::class.java.classLoader.getResourceAsStream(resource)).readText()

fun readResourceAsReader(resource: String) =
    InputStreamReader(QueryRecommendationTest::class.java.classLoader.getResourceAsStream(resource))

fun readJsonAsBsonDocument(resource: String) = BsonDocument.parse(readResourceAsString(resource))

val registry = CodecRegistries.fromProviders(
    DocumentCodecProvider(),
    BsonValueCodecProvider(),
    ValueCodecProvider()
)

fun toBsonDocument(document: Document) : BsonDocument {
    return document.toBsonDocument(BsonDocument::class.java, registry)
}

val formatter = ISODateTimeFormat.dateTime()

fun isoDateTimeStringToDateTime(value: String) : DateTime {
    return formatter.parseDateTime(value)
}