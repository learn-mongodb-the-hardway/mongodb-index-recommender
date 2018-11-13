package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.indexes.IdIndex
import com.mconsulting.indexrecommender.indexes.Index
import com.mconsulting.indexrecommender.ingress.Ingress
import com.mconsulting.indexrecommender.log.LogEntryBase
import com.mconsulting.indexrecommender.profiling.Operation
import com.mongodb.MongoClient

class Processor(
    val client: MongoClient,
    val namespaces: List<Namespace> = mutableListOf(),
    val collectionOptions: CollectionOptions = CollectionOptions()
) {
    private val ingressSources: MutableList<Ingress> = mutableListOf()
    private val dbs: MutableMap<String, Db> = mutableMapOf()
    private val namespaceLookup: MutableMap<String, Namespace> = mutableMapOf()

    init {
        namespaces.forEach {
            namespaceLookup[it.toString()] = it
        }
    }

    fun addSource(ingress: Ingress) {
        ingressSources += ingress
    }

    fun process() : IndexResults {
        namespaces.forEach {
            if (!dbs.containsKey(it.db)) {
                dbs[it.db] = Db(client, it, collectionOptions)
            }
        }

        // Process each of the ingress sources
        ingressSources.forEach {
            it.forEach(namespaces) {
                when (it) {
                    is LogEntryBase -> {
                        val collection = getCollection(it.namespace)
                        collection.addLogEntry(it)
                    }
                    is Operation -> {
                        val collection = getCollection(it.namespace())
                        collection.addOperation(it)
                    }
                }
            }
        }

        // Collect all the results from the processing
        // This will finalize all the entries and run index coalescence
        // on all the collections before returning the results
        return IndexResults(dbs.values.map { it.done() })
    }

    private fun getCollection(namespace: Namespace): Collection {
        // Grab the db object
        val db = dbs[namespace.db]!!
        // Grab (or create a collection object)
        return db.getCollection(namespace)
    }
}

class IndexResults(val dbIndexResults: List<DbIndexResult>) {
    fun getIndexes(namespace: Namespace): List<Index> {
        return dbIndexResults
            .filter { it.namespace.db == namespace.db }
            .flatMap { it.collectionIndexResults }
            .filter { it.namespace == namespace }
            .flatMap { it.indexes }
    }

    fun getIndex(namespace: Namespace, name: String): Index? {
        return getIndexes(namespace).firstOrNull { it.name == name }
    }
}