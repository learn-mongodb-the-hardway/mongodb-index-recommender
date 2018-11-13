package com.mconsulting.indexrecommender.ingress

import com.mconsulting.indexrecommender.IndexRecommendationEngine
import com.mconsulting.indexrecommender.Namespace
import com.mconsulting.indexrecommender.StatisticsProcessor
import com.mconsulting.indexrecommender.createOperation
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import org.bson.BsonDocument
import org.bson.BsonInt32

class ProfileCollectionIngress(
    val client: MongoClient,
    val namespace: Namespace
) : Ingress {
    private var systemProfileCollection: MongoCollection<BsonDocument>

    init {
        systemProfileCollection = client
            .getDatabase(namespace.db)
            .getCollection("system.profile", BsonDocument::class.java)
    }

    override fun forEach(namespaces: List<Namespace>, func: (value: Any) -> Unit) {
        systemProfileCollection.find().sort(BsonDocument()
            .append("ts", BsonInt32(IndexDirection.ASCENDING.value()))).map {
            createOperation(it)
        }.forEach {
            if (it != null) {
                func(it)
            }
        }
    }
}