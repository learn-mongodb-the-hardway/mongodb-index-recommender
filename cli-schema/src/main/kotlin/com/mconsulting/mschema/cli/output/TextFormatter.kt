package com.mconsulting.mschema.cli.output

import com.mconsulting.indexrecommender.CollectionIndexResults
import com.mconsulting.indexrecommender.DbIndexResult
import com.mconsulting.mschema.cli.IndentationWriter
import com.mconsulting.mschema.cli.writeln

class TextFormatter : Formatter() {

    override fun render(db: DbIndexResult, writer: IndentationWriter) {
        writer.writeln("db: ${db.namespace.db}")
        writer.writeln()
        writer.indent()

        db.collectionIndexResults.forEach { collection ->
            writer.writeln("collection: ${collection.namespace.collection}")
            writer.writeln()

            writer.indent()

            writer.writeln("notes:")
            writer.writeln()

            writer.indent()

            val notesWrittenCount = writeNotes(writer, collection)
            if (notesWrittenCount > 0) writer.writeln()

            writer.unIndent()

            writer.writeln("statistics:")
            writer.writeln()

            writer.indent()

            writer.writeln("document count: ${collection.collectionStats.count}")
            writer.writeln("index count: ${collection.indexes.size}")
            writer.writeln("read/write ratio: ${collection.collectionStats.readWriteRatio}")
            writer.writeln("ops:")

            writer.indent()

            writer.writeln("finds: ${collection.collectionStats.finds}")
            writer.writeln("updates: ${collection.collectionStats.updates}")
            writer.writeln("removes: ${collection.collectionStats.removes}")
            writer.writeln("inserts: ${collection.collectionStats.inserts}")

            writer.unIndent()

            writer.unIndent()

            writer.writeln()
            writer.writeln("indexes:")
            writer.writeln()

            writer.indent()

            collection.indexes.sortedBy { it.indexStatistics == null }.forEach { index ->
                writer.writeln(when (index.indexStatistics) {
                    null -> "<${indexTypeName(index)}>:"
                    else -> "[${indexTypeName(index)}]:"
                })
                writer.indent()

                writer.writeln("name: ${index.name}")
                writeIndexSpecific(writer, index)

                if (index.partialFilterExpression != null) {
                    writer.writeln("partialExpression: ${index.partialFilterExpression!!.toJson()}")
                }

                writer.writeln("unique: ${index.unique}")
                writer.writeln("sparse: ${index.sparse}")
                writer.writeln("statistics:")

                // Write out any index statistics
                writer.indent()

                // If we have MongoDB statistics print them out
                if (index.isExistingIndex()) {
                    writer.writeln("count: ${index.indexStatistics!!.ops}")
                    writer.writeln("since: ${index.indexStatistics!!.since}")
                } else {
                    writer.writeln("count: ${index.statistics.map { it.count }.sum()}")
                }

                writer.unIndent()

                // Do we have any shape statistics, render them
                if (index.statistics.isNotEmpty()) {
                    writer.writeln("shapes:")

                    writer.indent()

                    index.statistics.forEach {
                        writer.writeln("shape:")

                        writer.indent()

                        writer.writeln("filter: ${it.shape.toJsonString()}")
                        writer.writeln("count: ${it.count}")

                        writer.unIndent()
                    }

                    writer.unIndent()
                }

                // List any indexes removed
                if (index.removedIndexes.isNotEmpty()) {
                    writer.writeln("removed indexes:")

                    writer.indent()

                    index.removedIndexes.forEach { removedIndex ->
                        writer.writeln(when (removedIndex.indexStatistics) {
                            null -> "<${indexTypeName(removedIndex)}>:"
                            else -> "[${indexTypeName(removedIndex)}]:"
                        })

                        writer.indent()

                        writer.writeln("name: ${removedIndex.name}")
                        writeIndexSpecific(writer, index)

                        writer.unIndent()
                    }

                    writer.unIndent()
                }

                writer.unIndent()
                writer.writeln()
            }

            writer.unIndent()

            writer.unIndent()
        }
    }

    private fun writeNotes(writer: IndentationWriter, collection: CollectionIndexResults) : Int {
        var notesWrittenCount = 0

        // Low collection document count mean possibly less benefit from indexes
        if (collection.collectionStats.count < noteCollectionDocumentCountCutoff) {
            writer.writeln("""[number of documents < ${noteCollectionDocumentCountCutoff}]: The number of documents is ${collection.collectionStats.count} is relatively low meaning""")
            writer.indent()
            writer.writeln("that it's possible that using indexes might not provide a performance improvement")
            writer.writeln("over collection scans, especially if the collection fits in memory")
            writer.unIndent()
            notesWrittenCount += 1
        }

        // High read ratio -> more benefit of indexes
        if (collection.collectionStats.readWriteRatio > noteCollectionReadWriteRatioCutoff) {
            if (notesWrittenCount > 0) writer.writeln()
            writer.writeln("""[read/write ratio > ${noteCollectionReadWriteRatioCutoff}]: The read/write ratio is ${collection.collectionStats.readWriteRatio} is relatively high """)
            writer.indent()
            writer.writeln("meaning that the collection is mostly read from")
            writer.unIndent()
            notesWrittenCount += 1
        }

        // High write ratio -> possibly less benefits of indexes
        if (collection.collectionStats.readWriteRatio != 0.0
            && collection.collectionStats.readWriteRatio < noteCollectionReadWriteRatioWriteCutoff) {
            if (notesWrittenCount > 0) writer.writeln()
            writer.writeln("""[read/write ratio < ${noteCollectionReadWriteRatioWriteCutoff}]: The read/write ratio is ${collection.collectionStats.readWriteRatio} is relatively low """)
            writer.indent()
            writer.writeln("meaning that the collection is mostly written too")

            // Calculate insert/update and insert/remove ratios
            val insertUpdateRatio = collection.collectionStats.inserts.toDouble() / (collection.collectionStats.updates.toDouble() + 1)
            val insertRemoveRatio = collection.collectionStats.inserts.toDouble() / (collection.collectionStats.removes.toDouble() + 1)

            // Do we have ratios higher than the cutoff then we can suggest a lot of indexes might be bad
            if (insertUpdateRatio > insertToOtherWritesRatioCutOff
                && insertRemoveRatio > insertToOtherWritesRatioCutOff) {
                writer.writeln()
                writer.writeln("the insert/update ratio is ${insertUpdateRatio}")
                writer.writeln("the insert/remove ratio is ${insertRemoveRatio}")
                writer.writeln()
                writer.writeln("meaning that few write operations require an index")
                writer.writeln("having few indexes means less write amplification as")
                writer.writeln("fewer indexes must be updated for each write")
            }

            writer.unIndent()
            notesWrittenCount += 1
        }

        if (collection.indexes.size > noteCollectionIndexCountRatioCutoff) {
            if (notesWrittenCount > 0) writer.writeln()
            writer.writeln("""[index count > ${noteCollectionIndexCountRatioCutoff}]: The index count is ${collection.indexes.size} is relatively high.""")
            writer.indent()
            writer.writeln("Updating an index is not free of cost. A high number of collection indexes can")
            writer.writeln("cause significant additional write IO and latency to writes.")
            writer.unIndent()
            notesWrittenCount += 1
        }

        return notesWrittenCount
    }
}

val noteCollectionDocumentCountCutoff = 10000
val noteCollectionReadWriteRatioCutoff = 5
val noteCollectionIndexCountRatioCutoff = 10
val noteCollectionReadWriteRatioWriteCutoff = 0.2
val insertToOtherWritesRatioCutOff = 3