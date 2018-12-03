package com.mconsulting.indexrecommender.ingress

import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import com.mconsulting.indexrecommender.Namespace
import com.mconsulting.indexrecommender.createOperation
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import org.bson.BsonDocument
import org.bson.BsonInt32
import java.io.StringReader

data class ProfileCollectionIngressOptions(val quiet: Boolean = false)

class ProfileCollectionIngress(
    val client: MongoClient,
    val namespace: Namespace,
    val options: ProfileCollectionIngressOptions = ProfileCollectionIngressOptions()
) : Ingress {
    private var systemProfileCollection: MongoCollection<BsonDocument> = client
        .getDatabase(namespace.db)
        .getCollection("system.profile", BsonDocument::class.java)

    override fun forEach(namespaces: List<Namespace>, func: (value: Any) -> Unit) {
        systemProfileCollection.find().sort(BsonDocument()
            .append("ts", BsonInt32(IndexDirection.ASCENDING.value()))).map {
            val json = it.toJson()
            val obj = Parser().parse(StringReader(json))
            createOperation(obj as JsonObject, options.quiet)
        }.forEach {
            if (it != null) {
                func(it)
            }
        }
    }
}