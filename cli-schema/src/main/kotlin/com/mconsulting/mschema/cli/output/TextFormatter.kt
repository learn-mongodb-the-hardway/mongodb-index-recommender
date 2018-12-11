package com.mconsulting.mschema.cli.output

import com.mconsulting.indexrecommender.IndexResults
import com.mconsulting.mschema.cli.IndentationWriter
import com.mconsulting.mschema.cli.writeln

class TextFormatter(private val writer: IndentationWriter) : Formatter() {

    fun render(indexResults: IndexResults) {
        indexResults.dbIndexResults.forEach { db ->
            writer.writeln("db: ${db.namespace.db}")
            writer.writeln()
            writer.indent()

            db.collectionIndexResults.forEach { collection ->
                writer.writeln("collection: ${collection.namespace.collection}")
                writer.writeln()

                writer.indent()

                writer.writeln("statistics:")
                writer.writeln()

                writer.indent()

                writer.writeln("document count: ${collection.collectionStats.count}")
                writer.writeln("index count: ${collection.indexes.size}")

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

            writer.flush()
            writer.close()
        }
    }
}