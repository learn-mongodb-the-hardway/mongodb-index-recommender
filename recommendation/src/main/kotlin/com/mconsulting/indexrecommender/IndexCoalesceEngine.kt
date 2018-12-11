package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.indexes.CompoundIndex
import com.mconsulting.indexrecommender.indexes.CompoundTextIndex
import com.mconsulting.indexrecommender.indexes.Index
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.indexes.MultiFieldIndex
import com.mconsulting.indexrecommender.indexes.SingleFieldIndex
import com.mconsulting.indexrecommender.indexes.TextField
import com.mconsulting.indexrecommender.indexes.TextIndex
import java.util.*

data class IndexCoalesceResult(val indexes: List<Index>, val removedIndexes: List<Index> = listOf())

class IndexCoalesceEngine {

    fun coalesce(candidateIndexes: List<Index>): IndexCoalesceResult {
        if (candidateIndexes.isEmpty()) return IndexCoalesceResult(candidateIndexes)

        // Final indexes
        val indexes = mutableListOf<Index>()
        val removedIndexes = mutableListOf<Index>()

        // Split the indexes into different types
        val singleFieldIndexes = candidateIndexes.filterIsInstance<SingleFieldIndex>()
        val multiFieldIndexes = candidateIndexes.filterIsInstance<MultiFieldIndex>()
        val textIndexes = candidateIndexes.filterIsInstance<TextIndex>()
        val compoundTextIndexes = candidateIndexes.filterIsInstance<CompoundTextIndex>()

        // Any leftover indexes
        val restOfIndexes = candidateIndexes.filter {
            !singleFieldIndexes.contains(it)
                && !multiFieldIndexes.contains(it)
                && !textIndexes.contains(it)
        }

        // Check if the single field indexes are contained in the compound indexes
        processSingleFieldIndexes(singleFieldIndexes, multiFieldIndexes, removedIndexes, indexes)

        // Process text indexes
        processTextIndexes(textIndexes, compoundTextIndexes, removedIndexes, indexes)

        // Process compound indexes
        processCompoundIndexes(multiFieldIndexes, removedIndexes, indexes)

        // Return the indexes
        return IndexCoalesceResult(
            indexes + restOfIndexes, removedIndexes)
    }

    private fun processTextIndexes(textIndexes: List<TextIndex>, compoundTextIndexes: List<CompoundTextIndex>, removedIndexes: MutableList<Index>, indexes: MutableList<Index>) {
        val shapes = mutableListOf<ShapeStatistics>()
        val localRemovedIndexes = mutableListOf<Index>()

        // Do we have any text indexes
        if (textIndexes.isNotEmpty()) {
            val textIndexQueue = LinkedList(textIndexes.sortedBy { it.fields.size })
            val mergedFields = mutableSetOf<TextField>()

            // Merged the basic text index fields
            textIndexQueue.forEach {
                mergedFields += it.fields.filter { !mergedFields.contains(it) }
                localRemovedIndexes += it
                shapes += it.statistics
            }

            // Go over all the compound text indexes
            if (compoundTextIndexes.isNotEmpty()) {
                mergedFields += mergeCompoundTextIndexes(compoundTextIndexes, indexes, localRemovedIndexes, shapes)
            }

            // Merge removed indexes
            removedIndexes += localRemovedIndexes

            // Merge any compound text indexes, splitting them and merging them
            val index = TextIndex(createIndexName(mergedFields.toList()), mergedFields.toList())
            index.statistics += shapes
            index.removedIndexes += localRemovedIndexes
            indexes += index
            return
        }

        // Do we have more than one compound index, merge them
        if (compoundTextIndexes.size > 1) {
            val mergedFields = mergeCompoundTextIndexes(compoundTextIndexes, indexes, localRemovedIndexes, shapes)
            val index = TextIndex(createIndexName(mergedFields.toList()), mergedFields.toList())
            index.statistics += shapes
            index.removedIndexes += localRemovedIndexes
            indexes += index
        }

        // Merge removed indexes
        removedIndexes += localRemovedIndexes
    }

    private fun mergeCompoundTextIndexes(
        compoundTextIndexes: List<CompoundTextIndex>,
        indexes: MutableList<Index>,
        removedIndexes: MutableList<Index>,
        shapes: MutableList<ShapeStatistics>): List<TextField> {

        val mergedFields = mutableListOf<TextField>()
        // We need to merge any missing text fields
        mergedFields += compoundTextIndexes.map {
            it.fields.filter { !mergedFields.contains(it) }
        }.flatten()

        compoundTextIndexes.forEach { textIndex ->
            val filteredFields = textIndex.compoundFields.filter { field ->
                field.name !in listOf("_fts", "_ftsx")
            }

            // Add any shapes
            shapes += textIndex.statistics

            // Create a candidate compound index and add it if it does
            // not already exist, we are in fact splitting indexes here
            // due to there only being possible to have one text index per collection
            val compoundIndex = CompoundIndex(filteredFields.map { field ->
                "${field.name}_${when (field.direction) {
                    IndexDirection.ASCENDING, IndexDirection.DESCENDING -> field.direction.value()
                    else -> 1
                }}"
            }.joinToString("_"), filteredFields)
            // Add the compound index to the list of removed indexes
            compoundIndex.removedIndexes += textIndex

            if (!indexes.contains(compoundIndex)) {
                indexes += compoundIndex
            }
        }

        // Add all the compoundTextIndexes to the list of removed indexes (as they got merged)
        compoundTextIndexes.forEach {
            if (!removedIndexes.contains(it)) {
                removedIndexes += it
            }
        }

        // Return the merged fields
        return mergedFields
    }

    private fun createIndexName(mergedFields: List<TextField>): String {
        return mergedFields.map { "${it.path.joinToString(".")}_${it.weight}" }.joinToString("_")
    }

    private fun processSingleFieldIndexes(singleFieldIndexes: List<SingleFieldIndex>, compoundFieldIndexes: List<MultiFieldIndex>, removedIndexes: MutableList<Index>, indexes: MutableList<Index>) {
        // Get the distinct fields
        val singleIndexes = singleFieldIndexes.distinctBy {
            it.field.name
        }

        // Add any indexes that are not covered by a composite index
        indexes += singleIndexes.filter { singleFieldIndex ->
            val index = compoundFieldIndexes.firstOrNull { multiFieldIndex ->
                // Merge any statistics when we are removing fields
                if (multiFieldIndex.fields.first().name == singleFieldIndex.name) {
                    singleFieldIndex.statistics.forEach { shape ->
                        if (!multiFieldIndex.statistics.contains(shape)) {
                            multiFieldIndex.statistics += shape
                        } else {
                            multiFieldIndex.statistics.find { it == shape }!!.merge(shape)
                        }
                    }
                }

                multiFieldIndex.fields.first().name == singleFieldIndex.field.name && !singleFieldIndex.unique
            }

            // Add the single field index we are removing
            if (index != null) {
                index.removedIndexes += singleFieldIndex
            }

            index == null
        }

        // Add any removed indexes to the list of removed Indexes
        singleFieldIndexes.forEach {
            if (!indexes.contains(it)) {
                removedIndexes += it
            }
        }
    }

    private fun processCompoundIndexes(compoundFieldIndexes: List<MultiFieldIndex>, removedIndexes: MutableList<Index>, indexes: MutableList<Index>) {
        if (compoundFieldIndexes.isEmpty()) return
        val compoundFieldIndexList = LinkedList(compoundFieldIndexes.sortedBy { it.fields.size })

        // Go over all the indexes
        while (compoundFieldIndexList.isNotEmpty()) {
            val index = compoundFieldIndexList.pop()

            for (otherIndex in compoundFieldIndexList) {
                // If the indexes are not the same
                // And the fields list size of the other index is the same length or bigger
                // And the same size field list is the same we can remove the smaller index
                if (index != otherIndex && index.fields.size <= otherIndex.fields.size) {
                    if (index.fields == otherIndex.fields.subList(0, index.fields.size) && !index.unique) {
                        removedIndexes += index
                        otherIndex.removedIndexes += index

                        // Merge statistics
                        index.statistics.forEach { shape ->
                            if (!otherIndex.statistics.contains(shape)) {
                                otherIndex.statistics += shape
                            } else {
                                otherIndex.statistics.find { it == shape }!!.merge(shape)
                            }
                        }
                    }
                }
            }

            if (!removedIndexes.contains(index)) {
                indexes += index
            }
        }
    }
}