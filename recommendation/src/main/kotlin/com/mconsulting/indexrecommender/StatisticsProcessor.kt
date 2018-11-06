package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.profiling.Aggregation
import com.mconsulting.indexrecommender.profiling.Operation
import com.mconsulting.indexrecommender.profiling.Query
import org.bson.BsonArray
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonElement
import org.bson.BsonInt32

class ShapeStatistics(val shape: BsonDocument, var count: Long = 1) {
    override fun equals(other: Any?): Boolean {
        if (other == null) return false
        if (other !is ShapeStatistics) return false
        return this.shape.equals(other.shape)
    }

    fun merge(shapeStatistics: ShapeStatistics) {
        this.count += shapeStatistics.count
    }
}

class StatisticsProcessor() {
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
        val shapeStatistics = ShapeStatistics(filterShape)
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
