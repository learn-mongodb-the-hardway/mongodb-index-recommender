package com.mconsulting.indexrecommender

import com.beust.klaxon.JsonObject
import com.mconsulting.indexrecommender.indexes.Index
import com.mconsulting.indexrecommender.indexes.IndexParser
import com.mconsulting.indexrecommender.indexes.IndexParserOptions
import com.mconsulting.indexrecommender.indexes.IndexStatistics
import com.mconsulting.indexrecommender.log.LogEntry
import com.mconsulting.indexrecommender.profiling.Aggregation
import com.mconsulting.indexrecommender.profiling.ApplyOps
import com.mconsulting.indexrecommender.profiling.CollMod
import com.mconsulting.indexrecommender.profiling.CollStats
import com.mconsulting.indexrecommender.profiling.Count
import com.mconsulting.indexrecommender.profiling.Create
import com.mconsulting.indexrecommender.profiling.DataSize
import com.mconsulting.indexrecommender.profiling.Delete
import com.mconsulting.indexrecommender.profiling.DeleteIndexes
import com.mconsulting.indexrecommender.profiling.Distinct
import com.mconsulting.indexrecommender.profiling.Drop
import com.mconsulting.indexrecommender.profiling.Eval
import com.mconsulting.indexrecommender.profiling.Explain
import com.mconsulting.indexrecommender.profiling.FailedOperation
import com.mconsulting.indexrecommender.profiling.Find
import com.mconsulting.indexrecommender.profiling.FindAndModify
import com.mconsulting.indexrecommender.profiling.GeoNear
import com.mconsulting.indexrecommender.profiling.GeoSearch
import com.mconsulting.indexrecommender.profiling.GetMore
import com.mconsulting.indexrecommender.profiling.Group
import com.mconsulting.indexrecommender.profiling.Insert
import com.mconsulting.indexrecommender.profiling.KillCursors
import com.mconsulting.indexrecommender.profiling.ListIndexes
import com.mconsulting.indexrecommender.profiling.MapReduce
import com.mconsulting.indexrecommender.profiling.NotSupported
import com.mconsulting.indexrecommender.profiling.Operation
import com.mconsulting.indexrecommender.profiling.PlanCacheClear
import com.mconsulting.indexrecommender.profiling.PlanCacheClearFilters
import com.mconsulting.indexrecommender.profiling.PlanCacheListFilters
import com.mconsulting.indexrecommender.profiling.PlanCacheListPlans
import com.mconsulting.indexrecommender.profiling.PlanCacheListQueryShapes
import com.mconsulting.indexrecommender.profiling.PlanCacheSetFilter
import com.mconsulting.indexrecommender.profiling.Query
import com.mconsulting.indexrecommender.profiling.ReIndex
import com.mconsulting.indexrecommender.profiling.RenameCollection
import com.mconsulting.indexrecommender.profiling.Update
import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import mu.KLogging
import mu.KotlinLogging
import org.bson.BsonDocument
import org.bson.BsonElement
import org.bson.BsonString
import java.util.*

data class CollectionOptions(
    val allowExplainExecution: Boolean = true,
    val executeQueries: Boolean = true,
    val quiet: Boolean = false
)

data class CollectionIndexResults(
    val namespace: Namespace,
    val indexes: List<Index>,
    val shapeStatistics: List<ShapeStatistics>,
    val collectionStats: CollectionStats
) {
    fun getIndex(name: String): Index? {
        return indexes.firstOrNull { it.name == name }
    }

    fun contains(name: String): Boolean {
        return getIndex(name) != null
    }
}

class Collection(
    val client: MongoClient,
    val namespace: Namespace,
    val db: Db,
    options: CollectionOptions = CollectionOptions()) {

    val collectionStats: CollectionStats
    private var database: MongoDatabase = client.getDatabase(namespace.db)
    private var collection: MongoCollection<BsonDocument>
    private var existingIndexes: List<Index> = listOf()
    private var statisticsProcessor: StatisticsProcessor = StatisticsProcessor()

    // Index parser
    private val indexParser = IndexParser(client, IndexParserOptions(
        allowExplainExecution = options.allowExplainExecution
    ))

    // Recommendation engine
    private var recommendationEngine: IndexRecommendationEngine = IndexRecommendationEngine(client, statisticsProcessor, this, IndexRecommendationOptions(
        quiet = options.quiet
    ))

    init {
        collection = database.getCollection(namespace.collection, BsonDocument::class.java)

        // Read the collection stats
        collectionStats = when (database.listCollectionNames().contains(namespace.collection)) {
            true -> CollectionStats(database.runCommand(BsonDocument().append("collStats", BsonString(namespace.collection)), BsonDocument::class.java))
            false -> CollectionStats(BsonDocument())
        }

        // Process any existing indexes
        processExistingIndexes()
    }

    private fun processExistingIndexes() {
        // Read existing MongoDB Collection statistics information
        val indexStatistics = mutableListOf<BsonDocument>()

        collection.aggregate(listOf(
            BsonDocument(listOf(
                BsonElement("\$indexStats", BsonDocument())
            ))
        )).into(indexStatistics)

        // Read the existing indexes
        existingIndexes = collection.listIndexes(BsonDocument::class.java).map {
            val index = indexParser.createIndex(it)

            // Do we have an index statistic
            val indexStatistic = indexStatistics.firstOrNull {
                it.getString("name")!!.value == index.name
            }

            if (indexStatistic != null) {
                val accesses = indexStatistic.getDocument("accesses")
                val ops = accesses.getInt64("ops").value
                val since = Date(accesses.getDateTime("since").value)
                index.indexStatistics = IndexStatistics(ops, since)
            }

            index
        }.toList()

        // Feed the existing indexes to the recommendation engine
        existingIndexes.forEach {
            recommendationEngine.process(it)
        }
    }

    fun done() : CollectionIndexResults {
        return CollectionIndexResults(
            namespace = namespace,
            indexes = recommendationEngine.recommend().map {
                it.collectionStats = collectionStats
                it
            },
            shapeStatistics = statisticsProcessor.done(),
            collectionStats = collectionStats
        )
    }

    fun addLogEntry(logEntry: LogEntry) {
        recommendationEngine.process(logEntry)
    }

    fun addOperation(operation: Operation) {
        recommendationEngine.process(operation)
    }

    fun addIndex(index: Index) {
        recommendationEngine.addIndex(index)
    }

    companion object : KLogging()
}

private val logger = KotlinLogging.logger { }

fun createOperation(doc: JsonObject, quiet: Boolean = false): Operation? {
    val operation = doc.string("op")

    return when (operation) {
        "query" -> Query(doc)
        "insert" -> Insert(doc)
        "remove" -> Delete(doc)
        "update" -> Update(doc)
        "getmore" -> GetMore(doc)
        "killcursors" -> NotSupported(doc)
        "command" -> {
            // Did we get an exception
            if (doc.containsKey("exception") || doc.containsKey("errName")) {
                return FailedOperation(doc)
            }

            // Identify the operation
            // (we care only about operations that contain reads (query, agg, update, count etc.)
            val command = doc.obj("command")!!

            // Did the command get truncated
            if (command.containsKey("\$truncated")) {
                doc["exception"] = "Operation was truncated by MongoDB"
                return FailedOperation(doc)
            }

            return when {
                command.containsKey("aggregate") -> Aggregation(doc)
                command.containsKey("geoNear") -> GeoNear(doc)
                command.containsKey("count") -> Count(doc)
                command.containsKey("drop") -> Drop(doc)
                command.containsKey("group") -> Group(doc)
                command.containsKey("collStats") -> CollStats(doc)
                command.containsKey("listIndexes") -> ListIndexes(doc)
                command.containsKey("create") -> Create(doc)
                command.containsKey("deleteIndexes") -> DeleteIndexes(doc)
                command.containsKey("renameCollection") -> RenameCollection(doc)
                command.containsKey("findandmodify") ||
                command.containsKey("findAndModify") -> FindAndModify(doc)
                command.containsKey("datasize") -> DataSize(doc)
                command.containsKey("explain") -> Explain(doc)
                command.containsKey("find") -> Find(doc)
                command.containsKey("planCacheSetFilter") -> PlanCacheSetFilter(doc)
                command.containsKey("planCacheListPlans") -> PlanCacheListPlans(doc)
                command.containsKey("planCacheClear") -> PlanCacheClear(doc)
                command.containsKey("planCacheListFilters") -> PlanCacheListFilters(doc)
                command.containsKey("planCacheListQueryShapes") -> PlanCacheListQueryShapes(doc)
                command.containsKey("planCacheClearFilters") -> PlanCacheClearFilters(doc)
                command.containsKey("killCursors") -> KillCursors(doc)
                command.containsKey("distinct") -> Distinct(doc)
                command.containsKey("mapreduce") ||
                command.containsKey("mapReduce") -> MapReduce(doc)
                command.containsKey("reIndex") -> ReIndex(doc)
                command.containsKey("\$eval") -> Eval(doc)
                command.containsKey("collMod") -> CollMod(doc)
                command.containsKey("geoSearch") -> GeoSearch(doc)
                command.containsKey("applyOps") -> ApplyOps(doc)
                else -> {
                    if (!quiet) {
                        logger.warn { "Failed to create operation from [${doc.toJsonString()}]" }
                    }

                    NotSupported(doc)
                }
            }
        }
        else -> {
            if (!quiet) {
                logger.warn { "Failed to create operation from [${doc.toJsonString()}]" }
            }

            NotSupported(doc)
        }
    }
}