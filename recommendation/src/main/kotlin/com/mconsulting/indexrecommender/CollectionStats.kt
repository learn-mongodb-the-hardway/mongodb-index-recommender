package com.mconsulting.indexrecommender

import org.bson.BsonDocument

class CollectionStats(val document: BsonDocument) {
    private var cursor: BsonDocument = BsonDocument()

    init {
        if (document.containsKey("wiredTiger")) {
            val wiredTiger = document.getDocument("wiredTiger")!!

            if (wiredTiger.containsKey("cursor")) {
                cursor = wiredTiger.getDocument("cursor")!!
            }
        }
    }

    val count : Long
        get() = getLongDefault("count", document, 0)

    val inserts : Long = getLongMaybe("insert calls", cursor) ?: 0
    val updates : Long = getLongMaybe("update calls", cursor) ?: 0
    val finds : Long = getLongMaybe("next calls", cursor) ?: 0
    val removes : Long = getLongMaybe("remove calls", cursor) ?: 0
    val readWriteRatio: Double
        get() {
            // Reads
            val reads = finds
            val writes = inserts + updates +removes
            if (writes == 0L) return reads.toDouble()
            return reads.toDouble()/writes.toDouble()
        }
}