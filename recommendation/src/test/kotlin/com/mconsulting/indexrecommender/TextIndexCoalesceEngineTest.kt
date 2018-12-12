package com.mconsulting.indexrecommender

import com.mconsulting.indexrecommender.indexes.CompoundIndex
import com.mconsulting.indexrecommender.indexes.CompoundTextIndex
import com.mconsulting.indexrecommender.indexes.Field
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.indexes.TextField
import com.mconsulting.indexrecommender.indexes.TextIndex
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class TextIndexCoalesceEngineTest {
    private val coalesceEngine = IndexCoalesceEngine()

    @Test
    fun shouldMergeTwoTextIndexes() {
        val indexes = coalesceEngine.coalesce(listOf(
            TextIndex("a", listOf(
                TextField(listOf("a", "b"), 1)
            )),
            TextIndex("a", listOf(
                TextField(listOf("c", "b"), 10)
            ))
        ))

        assertEquals(1, indexes.indexes.size)
        assertEquals(2, indexes.removedIndexes.size)

        assertEquals(listOf(
            TextIndex("a", listOf(
                TextField(listOf("a", "b"), 1),
                TextField(listOf("c", "b"), 10)
            ))
        ), indexes.indexes)

        assertEquals(listOf(
            TextIndex("a", listOf(
                TextField(listOf("a", "b"), 1)
            )),
            TextIndex("a", listOf(
                TextField(listOf("c", "b"), 10)
            ))
        ), indexes.removedIndexes)
    }

    @Test
    fun shouldCorrectlyHandleCompoundTextIndexCombo() {
        val indexes = coalesceEngine.coalesce(listOf(
            TextIndex("a", listOf(
                TextField(listOf("c"), 1)
            )),
            CompoundTextIndex("b", listOf(
                Field("a", IndexDirection.ASCENDING),
                Field("b", IndexDirection.DESCENDING),
                Field("_fts", IndexDirection.UNKNOWN),
                Field("_ftsx", IndexDirection.ASCENDING)
            ), listOf(
                TextField(listOf("d"), 10)
            ))
        ))

        assertEquals(2, indexes.indexes.size)
        assertEquals(2, indexes.removedIndexes.size)

        assertEquals(listOf(
            CompoundIndex("a_1_b_-1", listOf(
                Field("a", IndexDirection.ASCENDING),
                Field("b", IndexDirection.DESCENDING)
            )),
            TextIndex("c_1_d_10", listOf(
                TextField(listOf("c"), 1),
                TextField(listOf("d"), 10)
            ))
        ), indexes.indexes)

        assertEquals(listOf(
            TextIndex("a", listOf(
                TextField(listOf("c"), 1)
            )),
            CompoundTextIndex("b", listOf(
                Field("a", IndexDirection.ASCENDING),
                Field("b", IndexDirection.DESCENDING),
                Field("_fts", IndexDirection.UNKNOWN),
                Field("_ftsx", IndexDirection.ASCENDING)
            ), listOf(
                TextField(listOf("d"), 10)
            ))
        ), indexes.removedIndexes)
    }

    @Test
    fun shouldMergeTwoCompoundTextIndexes() {
        val indexes = coalesceEngine.coalesce(listOf(
            CompoundTextIndex("a", listOf(
                Field("c", IndexDirection.ASCENDING),
                Field("b", IndexDirection.DESCENDING),
                Field("_fts", IndexDirection.UNKNOWN),
                Field("_ftsx", IndexDirection.ASCENDING)
            ), listOf(
                TextField(listOf("e"), 10)
            )),
            CompoundTextIndex("b", listOf(
                Field("a", IndexDirection.ASCENDING),
                Field("b", IndexDirection.DESCENDING),
                Field("_fts", IndexDirection.UNKNOWN),
                Field("_ftsx", IndexDirection.ASCENDING)
            ), listOf(
                TextField(listOf("d"), 10)
            ))
        ))

        assertEquals(3, indexes.indexes.size)
        assertEquals(2, indexes.removedIndexes.size)

        assertEquals(listOf(
            CompoundIndex("c_1_b_-1", listOf(
                Field("c", IndexDirection.ASCENDING),
                Field("b", IndexDirection.DESCENDING)
            )),
            CompoundIndex("a_1_b_-1", listOf(
                Field("a", IndexDirection.ASCENDING),
                Field("b", IndexDirection.DESCENDING)
            )),
            TextIndex("e_10_d_10", listOf(
                TextField(listOf("e"), 10),
                TextField(listOf("d"), 10)
            ))
        ), indexes.indexes)

        assertEquals(listOf(
            CompoundTextIndex("a", listOf(
                Field("c", IndexDirection.ASCENDING),
                Field("b", IndexDirection.DESCENDING),
                Field("_fts", IndexDirection.UNKNOWN),
                Field("_ftsx", IndexDirection.ASCENDING)
            ), listOf(
                TextField(listOf("e"), 10)
            )),
            CompoundTextIndex("b", listOf(
                Field("a", IndexDirection.ASCENDING),
                Field("b", IndexDirection.DESCENDING),
                Field("_fts", IndexDirection.UNKNOWN),
                Field("_ftsx", IndexDirection.ASCENDING)
            ), listOf(
                TextField(listOf("d"), 10)
            ))
        ), indexes.removedIndexes)
    }

    @Test
    fun shouldCorrectlyMergeTextIndexesWhenGlobalIndexExists() {
        val indexes = coalesceEngine.coalesce(listOf(
            TextIndex("a", listOf(
                TextField(listOf("c"), 1)
            )),
            CompoundTextIndex("b", listOf(
                Field("a", IndexDirection.ASCENDING),
                Field("b", IndexDirection.DESCENDING),
                Field("_fts", IndexDirection.UNKNOWN),
                Field("_ftsx", IndexDirection.ASCENDING)
            ), listOf(
                TextField(listOf("d"), 10)
            )),
            TextIndex("global", listOf(
                TextField(listOf("$**"), 1)
            ))
        ))

        assertEquals(2, indexes.indexes.size)
        assertEquals(3, indexes.removedIndexes.size)

        assertEquals(listOf(
            CompoundIndex("a_1_b_-1", listOf(
                Field("a", IndexDirection.ASCENDING),
                Field("b", IndexDirection.DESCENDING)
            )),
            TextIndex("\$**_1", listOf(
                TextField(listOf("\$**"), 1)
            ))
        ), indexes.indexes)

        assertEquals(listOf(
            TextIndex("a", listOf(
                TextField(listOf("c"), 1)
            )),
            CompoundTextIndex("b", listOf(
                Field("a", IndexDirection.ASCENDING),
                Field("b", IndexDirection.DESCENDING),
                Field("_fts", IndexDirection.UNKNOWN),
                Field("_ftsx", IndexDirection.ASCENDING)
            ), listOf(
                TextField(listOf("d"), 10)
            )),
            TextIndex("global", listOf(
                TextField(listOf("$**"), 1)
            ))
        ), indexes.removedIndexes)
    }
}