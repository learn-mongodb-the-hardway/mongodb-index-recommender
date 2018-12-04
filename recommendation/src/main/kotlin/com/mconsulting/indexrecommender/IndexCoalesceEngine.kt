package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.indexes.CompoundIndex
import com.mconsulting.indexrecommender.indexes.Index
import com.mconsulting.indexrecommender.indexes.SingleFieldIndex

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

        // Check if the single field indexes are contained in the compound indexes
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

//        // Filter out any duplicate indexes
//        val indexes = candidateIndexes.distinctBy {
//            it.
//        }

        return IndexCoalesceResult(indexes, removedIndexes)
    }
}