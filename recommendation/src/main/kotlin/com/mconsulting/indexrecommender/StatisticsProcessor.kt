package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.profiling.Aggregation
import com.mconsulting.indexrecommender.profiling.Operation
import com.mconsulting.indexrecommender.profiling.Query
import org.bson.BsonArray
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonElement
import org.bson.BsonInt32
import java.time.temporal.ChronoUnit
import java.time.temporal.Temporal
import java.util.*

class Frequency(var count: Long = 1)

class ShapeStatistics(val shape: BsonDocument, val timestamp: Long, var count: Long = 1, val resolution: TimeResolution) {
    val frequency = mutableMapOf<Long, Frequency>()

    init {
        // Adjust the timestamp for the bucket
        val adjustedTimestamp = adjustTimeStamp(timestamp, resolution)

        frequency[adjustedTimestamp] = Frequency()
    }

    override fun equals(other: Any?): Boolean {
        if (other == null) return false
        if (other !is ShapeStatistics) return false
        return this.shape.equals(other.shape)
    }

    fun merge(shapeStatistics: ShapeStatistics) {
        this.count += shapeStatistics.count

        // Adjust the timestamp for the bucket
        val adjustedTimestamp = adjustTimeStamp(shapeStatistics.timestamp, resolution)

        if (frequency.containsKey(adjustedTimestamp)) {
            frequency[adjustedTimestamp]!!.count += shapeStatistics.count
        } else {
            frequency[adjustedTimestamp] = Frequency(shapeStatistics.count)
        }
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

    fun process(operation: Operation) {
        when (operation) {
            is Query -> processQuery(operation)
            is Aggregation -> processAggregation(operation)
        }
    }

    private fun processQuery(operation: Query) {
        // Turn the query filter into a fixed shape (all basic values are changed to 1's)
        val filterShape = normalizeFilter(operation.command().filter)
        // Create a statistics shape
        val shapeStatistics = ShapeStatistics(
            shape = filterShape,
            timestamp = operation.timestamp().value,
            resolution = options.bucketResolution)
        // Check if the filter exists
        if (!shapes.contains(shapeStatistics)) {
            shapes += shapeStatistics
        } else {
            shapes.find { it == shapeStatistics }!!.merge(shapeStatistics)
        }
    }

    private fun processAggregation(operation: Aggregation) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

private fun normalizeFilter(filter: BsonDocument): BsonDocument {
    return BsonDocument(filter.entries.map {
        val value = it.value

        BsonElement(it.key, when (value) {
            is BsonDocument -> normalizeFilter(value)
            is BsonArray -> normalizeArray(value)
            else -> BsonBoolean(true)
        })
    })
}

private fun normalizeArray(value: BsonArray): BsonArray {
    return BsonArray(value.values.map {
        when (it) {
            is BsonDocument -> normalizeFilter(it)
            is BsonArray -> normalizeArray(it)
            else -> BsonInt32(1)
        }
    })
}
