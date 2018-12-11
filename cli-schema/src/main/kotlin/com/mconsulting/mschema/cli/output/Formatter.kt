package com.mconsulting.mschema.cli.output

import com.mconsulting.indexrecommender.DbIndexResult
import com.mconsulting.indexrecommender.indexes.CompoundIndex
import com.mconsulting.indexrecommender.indexes.HashedIndex
import com.mconsulting.indexrecommender.indexes.IdIndex
import com.mconsulting.indexrecommender.indexes.Index
import com.mconsulting.indexrecommender.indexes.MultikeyIndex
import com.mconsulting.indexrecommender.indexes.SingleFieldIndex
import com.mconsulting.indexrecommender.indexes.TTLIndex
import com.mconsulting.indexrecommender.indexes.TextIndex
import com.mconsulting.indexrecommender.indexes.TwoDIndex
import com.mconsulting.indexrecommender.indexes.TwoDSphereIndex
import com.mconsulting.mschema.cli.IndentationWriter
import com.mconsulting.mschema.cli.writeln
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString

abstract class Formatter {
    abstract fun render(indexResult: DbIndexResult, writer: IndentationWriter)

    protected fun writeIndexSpecificJson(index: Index) : BsonDocument {
        val document = BsonDocument()

        when (index) {
            is SingleFieldIndex -> {
                document.append("field", BsonString(index.field.name))
                document.append("direction", BsonString(index.field.direction.toString()))
            }
            is CompoundIndex -> {
                document.append("fields", BsonArray(index.fields.map {
                    BsonDocument()
                        .append("field", BsonString(it.name))
                        .append("direction", BsonString(it.direction.toString()))
                }))
            }
            is HashedIndex -> BsonDocument().append("field", BsonString(index.field))
            is TwoDSphereIndex -> BsonDocument().append("field", BsonString(index.key))
            is TwoDIndex -> BsonDocument().append("field", BsonString(index.key))
            is MultikeyIndex -> {
                document.append("fields", BsonArray(index.fields.map {
                    BsonDocument()
                        .append("field", BsonString(it.name))
                        .append("direction", BsonString(it.direction.toString()))
                }))
            }
            is TextIndex -> {
                document.append("fields", BsonArray(index.fields.map {
                    BsonDocument()
                        .append("field", BsonString(it.path.joinToString(".")))
                        .append("weight", BsonInt32(it.weight))
                }))
            }
            is TTLIndex -> {

            }
        }

        return document
    }

    protected fun writeIndexSpecific(writer: IndentationWriter, index: Index) {
        when (index) {
            is SingleFieldIndex -> {
                writer.writeln("field:")
                writer.indent()

                writer.writeln("key: ${index.field.name}")
                writer.writeln("direction: ${index.field.direction}")

                writer.unIndent()
            }
            is CompoundIndex -> {
                writer.writeln("fields:")
                writer.indent()

                index.fields.forEach {
                    writer.writeln("field:")
                    writer.indent()

                    writer.writeln("key: ${it.name}")
                    writer.writeln("direction: ${it.direction}")

                    writer.unIndent()
                }

                writer.unIndent()
            }
            is HashedIndex -> writer.writeln("field: ${index.field}")
            is TwoDSphereIndex -> writer.writeln("field: ${index.key}")
            is TwoDIndex -> writer.writeln("field: ${index.key}")
            is MultikeyIndex -> {
                writer.writeln("fields:")
                writer.indent()

                index.fields.forEach {
                    writer.writeln("field:")
                    writer.indent()

                    writer.writeln("key: ${it.name}")
                    writer.writeln("direction: ${it.direction}")

                    writer.unIndent()
                }

                writer.unIndent()
            }
            is TextIndex -> {
                writer.writeln("fields:")
                writer.indent()

                index.fields.forEach {
                    writer.writeln("field:")
                    writer.indent()

                    writer.writeln("key: ${it.path}")
                    writer.writeln("weight: ${it.weight}")

                    writer.unIndent()
                }

                writer.unIndent()
            }
            is TTLIndex -> {

            }
        }
    }

    protected fun indexTypeName(index: Index): String {
        return when (index) {
            is SingleFieldIndex -> "Single Key"
            is CompoundIndex -> "Compound Key"
            is HashedIndex -> "Hashed"
            is MultikeyIndex -> "Multi Key"
            is IdIndex -> "Id"
            is TwoDSphereIndex -> "2d Sphere"
            is TwoDIndex -> "2d"
            is TextIndex -> "Text"
            is TTLIndex -> "Time To Live"
            else -> "Unknown"
        }
    }
}