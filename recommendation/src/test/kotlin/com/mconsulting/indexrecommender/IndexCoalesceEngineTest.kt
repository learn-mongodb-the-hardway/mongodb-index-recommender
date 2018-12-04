package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.indexes.CompoundIndex
import com.mconsulting.indexrecommender.indexes.Field
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.indexes.SingleFieldIndex
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class IndexCoalesceEngineTest {
    private val coalesceEngine = IndexCoalesceEngine()

    @Test
    fun shouldCorrectlyRemoveSingleFieldIndex() {
        val indexes = coalesceEngine.coalesce(listOf(
            SingleFieldIndex("a", Field("a", IndexDirection.ASCENDING)),
            CompoundIndex("b", listOf(
                Field("a", IndexDirection.ASCENDING),
                Field("b", IndexDirection.DESCENDING)
            ))
        ))

        assertEquals(1, indexes.indexes.size)
        assertEquals(1, indexes.removedIndexes.size)

        assertEquals(listOf(
            CompoundIndex("b", listOf(
                Field("a", IndexDirection.ASCENDING),
                Field("b", IndexDirection.DESCENDING)
            ))
        ), indexes.indexes)

        assertEquals(listOf(
            SingleFieldIndex("a", Field("a", IndexDirection.ASCENDING))
        ), indexes.removedIndexes)
    }

    @Test
    fun shouldCorrectlyRemoveCompoundFieldIndex() {
        val indexes = coalesceEngine.coalesce(listOf(
            CompoundIndex("b", listOf(
                Field("a", IndexDirection.ASCENDING),
                Field("b", IndexDirection.DESCENDING)
            )),
            CompoundIndex("b", listOf(
                Field("a", IndexDirection.ASCENDING),
                Field("b", IndexDirection.DESCENDING),
                Field("c", IndexDirection.DESCENDING)
            ))
        ))

        assertEquals(1, indexes.indexes.size)
        assertEquals(1, indexes.removedIndexes.size)

        assertEquals(listOf(
            CompoundIndex("b", listOf(
                Field("a", IndexDirection.ASCENDING),
                Field("b", IndexDirection.DESCENDING),
                Field("c", IndexDirection.DESCENDING)
            ))
        ), indexes.indexes)

        assertEquals(listOf(
            CompoundIndex("b", listOf(
                Field("a", IndexDirection.ASCENDING),
                Field("b", IndexDirection.DESCENDING)
            ))
        ), indexes.removedIndexes)
    }
}