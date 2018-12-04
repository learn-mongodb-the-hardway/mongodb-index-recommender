package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.indexes.CompoundIndex
import com.mconsulting.indexrecommender.indexes.Index
import com.mconsulting.indexrecommender.indexes.SingleFieldIndex
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
        val compoundFieldIndexes = candidateIndexes.filterIsInstance<CompoundIndex>()

        // Any leftover indexes
        val restOfIndexes = candidateIndexes.filter {
            !singleFieldIndexes.contains(it) && !compoundFieldIndexes.contains(it)
        }

        // Check if the single field indexes are contained in the compound indexes
        processSingleFieldIndexes(singleFieldIndexes, compoundFieldIndexes, removedIndexes, indexes)

        // List of indexes we are keeping
        val keptCompoundIndexes = processCompoundIndexes(compoundFieldIndexes, removedIndexes)

        // Return the indexes
        return IndexCoalesceResult(
            indexes + keptCompoundIndexes + restOfIndexes, removedIndexes)
    }

    private fun processSingleFieldIndexes(singleFieldIndexes: List<SingleFieldIndex>, compoundFieldIndexes: List<CompoundIndex>, removedIndexes: MutableList<Index>, indexes: MutableList<Index>) {
        singleFieldIndexes.forEach { singleFieldIndex ->
            for (index in compoundFieldIndexes) {
                if (index.fields.first() == singleFieldIndex.field) {
                    removedIndexes += singleFieldIndex
                    break
                }
            }

            if (!removedIndexes.contains(singleFieldIndex)) {
                indexes += singleFieldIndex
            }
        }
    }

    private fun processCompoundIndexes(compoundFieldIndexes: List<CompoundIndex>, removedIndexes: MutableList<Index>): MutableList<CompoundIndex> {
        val compoundFieldIndexList = LinkedList(compoundFieldIndexes.sortedBy { it.fields.size })
        val keptCompoundIndexes = mutableListOf<CompoundIndex>()

        // Go over all the indexes
        while (compoundFieldIndexList.isNotEmpty()) {
            val index = compoundFieldIndexList.pop()

            for (otherIndex in compoundFieldIndexList) {
                // If the indexes are not the same
                // And the fields list size of the other index is the same length or bigger
                // And the same size field list is the same we can remove the smaller index
                if (index != otherIndex && index.fields.size <= otherIndex.fields.size) {
                    if (index.fields == otherIndex.fields.subList(0, index.fields.size)) {
                        removedIndexes += index
                    }
                }
            }

            if (!removedIndexes.contains(index)) {
                keptCompoundIndexes += index
            }
        }

        return keptCompoundIndexes
    }
}