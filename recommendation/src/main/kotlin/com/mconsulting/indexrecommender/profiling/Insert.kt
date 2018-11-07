package com.mconsulting.indexrecommender.profiling

import org.bson.BsonDocument

class Insert(doc: BsonDocument) : WriteOperation(doc) {
    fun numberInserted() = doc.getInt32("ninserted")
}