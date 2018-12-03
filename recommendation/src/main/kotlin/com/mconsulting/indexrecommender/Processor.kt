package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.indexes.Index
import com.mconsulting.indexrecommender.ingress.Ingress
import com.mconsulting.indexrecommender.log.LogEntryBase
import com.mconsulting.indexrecommender.profiling.FailedOperation
import com.mconsulting.indexrecommender.profiling.NotSupported
import com.mconsulting.indexrecommender.profiling.Operation
import com.mongodb.MongoClient
import mu.KLogging

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
        ingressSources.forEach {ingress ->
            ingress.forEach(namespaces) { operation ->
                when (operation) {
                    is LogEntryBase -> {
                        val collection = getCollection(operation.namespace)
                        collection.addLogEntry(operation)
                    }
                    is NotSupported -> logger.warn { "Attempting to process a non supported operation" }
                    is FailedOperation -> logger.warn { "Attempting to process a operation that threw an exception" }
                    is Operation -> {
                        val collection = getCollection(operation.namespace())
                        collection.addOperation(operation)
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
        val db = getDatabase(namespace)
        // Grab (or create a collection object)
        return db.getCollection(namespace)
    }

    private fun getDatabase(namespace: Namespace): Db {
        if (!dbs.containsKey(namespace.db)) {
            dbs[namespace.db] = Db(client, namespace, collectionOptions)
        }

        return dbs[namespace.db]!!
    }

    companion object : KLogging()
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

    fun contains(namespace: Namespace, name: String): Boolean {
        return getIndex(namespace, name) != null
    }
}