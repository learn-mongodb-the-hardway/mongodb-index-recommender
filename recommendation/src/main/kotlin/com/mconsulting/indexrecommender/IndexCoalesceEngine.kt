package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.indexes.Index
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

        // Any leftover indexes
        val restOfIndexes = candidateIndexes.filter {
            !singleFieldIndexes.contains(it)
                && !multiFieldIndexes.contains(it)
                && !textIndexes.contains(it)
        }

        // Check if the single field indexes are contained in the compound indexes
        processSingleFieldIndexes(singleFieldIndexes, multiFieldIndexes, removedIndexes, indexes)

        // Process compound indexes
        processCompoundIndexes(multiFieldIndexes, removedIndexes, indexes)

        // Process text indexes
        processTextIndexes(textIndexes, removedIndexes, indexes)

        // Return the indexes
        return IndexCoalesceResult(
            indexes + restOfIndexes, removedIndexes)
    }

    private fun processTextIndexes(textIndexes: List<TextIndex>, removedIndexes: MutableList<Index>, indexes: MutableList<Index>) {
        if (textIndexes.isEmpty()) return

        val textIndexQueue = LinkedList(textIndexes.sortedBy { it.fields.size })
        val mergedFields = mutableListOf<TextField>()

        textIndexQueue.forEach {
            mergedFields += it.fields.filter { !mergedFields.contains(it) }
            removedIndexes += it
        }

        indexes += TextIndex(createIndexName(mergedFields), mergedFields)
    }

    private fun createIndexName(mergedFields: MutableList<TextField>): String {
        return mergedFields.map { "${it.path.joinToString(".")}_${it.weight}" }.joinToString("_")
    }

    private fun processSingleFieldIndexes(singleFieldIndexes: List<SingleFieldIndex>, compoundFieldIndexes: List<MultiFieldIndex>, removedIndexes: MutableList<Index>, indexes: MutableList<Index>) {
        singleFieldIndexes.forEach { singleFieldIndex ->
            for (index in compoundFieldIndexes) {
                if (index.fields.first() == singleFieldIndex.field && !singleFieldIndex.unique) {
                    removedIndexes += singleFieldIndex
                    break
                }
            }

            if (!removedIndexes.contains(singleFieldIndex)) {
                indexes += singleFieldIndex
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