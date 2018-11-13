package com.mconsulting.indexrecommender

import com.mongodb.MongoClient

class Db(
    val client: MongoClient,
    val namespace: Namespace,
    val collectionOptions: CollectionOptions = CollectionOptions()
) {
    val collections: MutableMap<String, Collection> = mutableMapOf()

    fun getCollection(namespace: Namespace): Collection {
        if (collections.containsKey(namespace.collection)) {
            return collections[namespace.collection]!!
        }

        // Add the missing collection information
        collections[namespace.collection] = Collection(client, namespace, this, collectionOptions)
        return collections[namespace.collection]!!
    }

    fun done(): DbIndexResult {
        return DbIndexResult(namespace, collections.values.map {
            it.done()
        })
    }
}

class DbIndexResult(val namespace: Namespace, val collectionIndexResults: List<CollectionIndexResults>)