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
        // Do we have any text indexes
        if (textIndexes.isNotEmpty()) {
            val textIndexQueue = LinkedList(textIndexes.sortedBy { it.fields.size })
            val mergedFields = mutableSetOf<TextField>()

            // Merged the basic text index fields
            textIndexQueue.forEach {
                mergedFields += it.fields.filter { !mergedFields.contains(it) }
                removedIndexes += it
            }

            // Go over all the compound text indexes
            if (compoundTextIndexes.isNotEmpty()) {
                mergedFields += mergeCompoundIndexes(compoundTextIndexes, indexes)
            }

            // Merge any compound text indexes, splitting them and merging them
            indexes += TextIndex(createIndexName(mergedFields.toList()), mergedFields.toList())
            return
        }

        // Do we have more than one compound index, merge them
        if (compoundTextIndexes.size > 1) {
            val mergedFields = mergeCompoundIndexes(compoundTextIndexes, indexes)
            indexes += TextIndex(createIndexName(mergedFields.toList()), mergedFields.toList())
        }
    }

    private fun mergeCompoundIndexes(compoundTextIndexes: List<CompoundTextIndex>, indexes: MutableList<Index>): List<TextField> {
        val mergedFields = mutableListOf<TextField>()
        // We need to merge any missing text fields
        mergedFields += compoundTextIndexes.map {
            it.fields.filter { !mergedFields.contains(it) }
        }.flatten()

        compoundTextIndexes.forEach {
            val filteredFields = it.compoundFields.filter {
                it.name !in listOf("_fts", "_ftsx")
            }
            // Create a candidate compound index and add it if it does
            // not already exist, we are in fact splitting indexes here
            // due to there only being possible to have one text index per collection
            val compoundIndex = CompoundIndex(filteredFields.map {
                "${it.name}_${when (it.direction) {
                    IndexDirection.ASCENDING, IndexDirection.DESCENDING -> it.direction.value()
                    else -> 1
                }}"
            }.joinToString("_"), filteredFields)

            if (!indexes.contains(compoundIndex)) {
                indexes += compoundIndex
            }
        }

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
            compoundFieldIndexes.firstOrNull { multiFieldIndex ->
                multiFieldIndex.fields.first().name == singleFieldIndex.name
            } == null
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
                    }
                }
            }

            if (!removedIndexes.contains(index)) {
                indexes += index
            }
        }
    }
}