package com.mconsulting.indexrecommender

import com.beust.klaxon.JsonArray
import com.beust.klaxon.JsonBase
import com.beust.klaxon.JsonObject
import com.beust.klaxon.JsonValue
import com.mconsulting.indexrecommender.log.CommandLogEntry
import com.mconsulting.indexrecommender.log.LogEntry
import com.mconsulting.indexrecommender.profiling.Aggregation
import com.mconsulting.indexrecommender.profiling.Delete
import com.mconsulting.indexrecommender.profiling.Insert
import com.mconsulting.indexrecommender.profiling.Operation
import com.mconsulting.indexrecommender.profiling.Query
import com.mconsulting.indexrecommender.profiling.ReadOperation
import com.mconsulting.indexrecommender.profiling.Update
import org.apache.commons.math3.stat.descriptive.moment.Mean
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation
import org.apache.commons.math3.stat.descriptive.moment.Variance
import org.apache.commons.math3.stat.descriptive.rank.Max
import org.apache.commons.math3.stat.descriptive.rank.Min
import org.bson.BsonArray
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonElement
import org.bson.BsonInt32
import org.bson.BsonValue
import java.time.temporal.ChronoUnit
import java.util.*

class StatHolder {
    val max = Max()
    val min = Min()
    val mean = Mean()
    val variance = Variance()
    val stdDev = StandardDeviation()

    fun increment(value: Double) {
        max.increment(value)
        min.increment(value)
        mean.increment(value)
        variance.increment(value)
        stdDev.increment(value)
    }
}

class BucketStatistics(
    var count: Long, var responseLength: Double, var millis: Double
) {
    val responseLengthStat = StatHolder()
    val millisStat = StatHolder()

    init {
        responseLengthStat.increment(responseLength)
        millisStat.increment(millis)
    }

    fun merge(statistics: BucketStatistics) {
        count += statistics.count

        responseLengthStat.increment(statistics.responseLength)
        millisStat.increment(statistics.millis)
    }
}

enum class MongoOperation {
    INSERT, UPDATE, AGGREGATION, QUERY, DELETE, OTHER
}

private fun mapOperation(operation: Operation): MongoOperation {
    return when (operation) {
        is Insert -> MongoOperation.INSERT
        is Update -> MongoOperation.UPDATE
        is Query -> MongoOperation.QUERY
        is Aggregation -> MongoOperation.AGGREGATION
        is Delete -> MongoOperation.DELETE
        else -> MongoOperation.OTHER
    }
}

private fun mapOperation(operation: String): MongoOperation {
    return when (operation) {
        "aggregate" -> MongoOperation.AGGREGATION
        else -> MongoOperation.OTHER
    }
}

class ShapeStatistics(
    val operation: MongoOperation,
    val shape: JsonBase,
    val timestamp: Long,
    var count: Long = 1,
    var responseLength: Double,
    var millis: Double,
    val resolution: TimeResolution) {

    // Statistical storage
    val frequency = mutableMapOf<Long, BucketStatistics>()
    val responseLengthStat = StatHolder()
    val millisStat = StatHolder()

    init {
        // Adjust the timestamp for the bucket
        val adjustedTimestamp = adjustTimeStamp(timestamp, resolution)

        // Add the frequencey entry
        frequency[adjustedTimestamp] = BucketStatistics(
            count = 1,
            responseLength = responseLength,
            millis = millis
        )

        // Add to Global statistcs
        responseLengthStat.increment(responseLength)
        millisStat.increment(millis)
    }

    override fun equals(other: Any?): Boolean {
        if (other == null) return false
        if (other !is ShapeStatistics) return false
        return this.shape == other.shape
    }

    fun merge(shapeStatistics: ShapeStatistics) {
        // Adjust the timestamp for the bucket
        val adjustedTimestamp = adjustTimeStamp(shapeStatistics.timestamp, resolution)

        if (frequency.containsKey(adjustedTimestamp)) {
            frequency[adjustedTimestamp]!!.merge(shapeStatistics.frequency.values.first())
        } else {
            frequency[adjustedTimestamp] = BucketStatistics(
                count = shapeStatistics.count,
                responseLength = shapeStatistics.responseLength,
                millis = shapeStatistics.millis
            )
        }

        // Adjust the overal shape statistics
        this.count += shapeStatistics.count
        this.responseLengthStat.increment(shapeStatistics.responseLength)
        this.millisStat.increment(shapeStatistics.millis)
    }

    private fun adjustTimeStamp(timestamp: Long, resolution: TimeResolution): Long {
        return when (resolution) {
            TimeResolution.MILLISECOND -> timestamp
            TimeResolution.SECOND -> {
                Date(timestamp).toInstant().truncatedTo(ChronoUnit.SECONDS).toEpochMilli()
            }
            TimeResolution.MINUTE -> {
                Date(timestamp).toInstant().truncatedTo(ChronoUnit.MINUTES).toEpochMilli()
            }
            TimeResolution.HOUR -> {
                Date(timestamp).toInstant().truncatedTo(ChronoUnit.HOURS).toEpochMilli()
            }
            TimeResolution.DAY -> {
                Date(timestamp).toInstant().truncatedTo(ChronoUnit.DAYS).toEpochMilli()
            }
        }
    }
}

enum class TimeResolution {
    MILLISECOND, SECOND, MINUTE, HOUR, DAY
}

data class StatisticsProcessorOptions(val bucketResolution: TimeResolution = TimeResolution.SECOND)

class StatisticsProcessor(val options: StatisticsProcessorOptions = StatisticsProcessorOptions()) {
    val shapes: MutableList<ShapeStatistics> = mutableListOf()
    var inserts: ShapeStatistics? = null

    fun process(operation: Operation) {
        when (operation) {
            is Query -> processShape(normalizeFilter(operation.command().filter), operation)
            is Aggregation -> processShape(normalizeFilter(operation.command().pipeline), operation)
            is Insert -> processInsert(operation)
            is Update -> processShape(normalizeFilter(operation.command().query), operation)
            is Delete -> processShape(normalizeFilter(operation.command().query), operation)
        }
    }

    fun process(operation: LogEntry) {
        when (operation) {
            is CommandLogEntry -> processCommandLogEntry(operation)
        }
    }

    private fun processCommandLogEntry(logEntry: CommandLogEntry) {
        when (logEntry.commandName) {
            "aggregate" -> {
                processShape(normalizeFilter(logEntry.command.array<JsonObject>("pipeline")!!), logEntry)
            }
        }
    }

    private fun processInsert(operation: Insert) {
        // Create a statistics shape
        val shapeStatistics = ShapeStatistics(
            operation = MongoOperation.INSERT,
            shape = JsonObject(),
            timestamp = operation.timestamp(),
            responseLength = 0.0,
            resolution = options.bucketResolution,
            millis = operation.milis().toDouble()
        )

        if (inserts == null) {
            inserts = shapeStatistics
        } else {
            inserts!!.merge(shapeStatistics)
        }
    }

    private fun processShape(shape: JsonBase, logEntry: LogEntry) {
        when (logEntry) {
            is CommandLogEntry -> {
                val responseLength = logEntry.resultLength

                // Create a statistics shape
                val shapeStatistics = ShapeStatistics(
                    operation = mapOperation(logEntry.commandName),
                    shape = shape,
                    timestamp = logEntry.timestamp.millis,
                    responseLength = responseLength.toDouble(),
                    resolution = options.bucketResolution,
                    millis = logEntry.executionTimeMS.toDouble()
                )

                // Check if the filter exists
                if (!shapes.contains(shapeStatistics)) {
                    shapes += shapeStatistics
                } else {
                    shapes.find { it == shapeStatistics }!!.merge(shapeStatistics)
                }
            }
        }
    }

    private fun processShape(shape: JsonBase, operation: Operation) {
        val responseLength = when (operation) {
            is ReadOperation -> operation.responseLength()
            else -> 0
        }

        // Create a statistics shape
        val shapeStatistics = ShapeStatistics(
            operation = mapOperation(operation),
            shape = shape,
            timestamp = operation.timestamp(),
            responseLength = responseLength.toDouble(),
            resolution = options.bucketResolution,
            millis = operation.milis().toDouble()
        )

        // Check if the filter exists
        if (!shapes.contains(shapeStatistics)) {
            shapes += shapeStatistics
        } else {
            shapes.find { it == shapeStatistics }!!.merge(shapeStatistics)
        }
    }

    fun done(): List<ShapeStatistics> {
        val finalShapes = shapes.toMutableList()
        if (inserts != null) finalShapes += inserts!!
        return shapes
    }
}

private fun normalizeFilter(value: JsonBase) : JsonBase {
    return when (value) {
        is JsonObject -> normalizeDocument(value)
        is JsonArray<*> -> normalizeArray(value)
        else -> value
    }
}

private fun normalizeDocument(filter: JsonObject): JsonObject {
    return JsonObject(filter.entries.map {
        val value = it.value

        it.key to when (value) {
            is JsonObject -> normalizeDocument(value)
            is JsonArray<*> -> normalizeArray(value)
            else ->true
        }
    }.toMap())
}

private fun normalizeArray(value: JsonArray<*>): JsonArray<*> {
    return JsonArray(value.map {
        when (it) {
            is JsonObject -> normalizeDocument(it)
            is JsonArray<*> -> normalizeArray(it)
            else -> 1
        }
    })
}
