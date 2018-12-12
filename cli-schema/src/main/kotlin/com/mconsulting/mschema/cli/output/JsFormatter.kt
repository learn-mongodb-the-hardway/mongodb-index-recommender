package com.mconsulting.mschema.cli.output

import com.mconsulting.indexrecommender.DbIndexResult
import com.mconsulting.indexrecommender.indexes.CompoundIndex
import com.mconsulting.indexrecommender.indexes.CompoundTextIndex
import com.mconsulting.indexrecommender.indexes.Field
import com.mconsulting.indexrecommender.indexes.HashedIndex
import com.mconsulting.indexrecommender.indexes.IndexDirection
import com.mconsulting.indexrecommender.indexes.MultikeyIndex
import com.mconsulting.indexrecommender.indexes.SingleFieldIndex
import com.mconsulting.indexrecommender.indexes.TTLIndex
import com.mconsulting.indexrecommender.indexes.TextField
import com.mconsulting.indexrecommender.indexes.TextIndex
import com.mconsulting.indexrecommender.indexes.TwoDIndex
import com.mconsulting.indexrecommender.indexes.TwoDSphereIndex
import com.mconsulting.mschema.cli.IndentationWriter
import com.mconsulting.mschema.cli.writeln
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonElement
import org.bson.BsonInt32
import org.bson.BsonString

class JsFormatter : Formatter() {
    override fun render(db: DbIndexResult, writer: IndentationWriter) {
        writer.writeln("// Select the database [${db.namespace.db}]")
        writer.writeln("var db = db.getSiblingDB(\"${db.namespace.db}\");")

        // For each collection let's first drop indexes and then create the new ones
        db.collectionIndexResults.forEach { collectionResult ->
            writer.writeln()
            writer.writeln("// Drop and create indexes for collection ${collectionResult.namespace.collection}")

            collectionResult.indexes.forEach { index ->
                // Create drop index statements
                index.removedIndexes.forEach { removedIndex ->
                    writer.writeln("db.${collectionResult.namespace.collection}.dropIndex(\"${removedIndex.name}\");")
                }

                // Create create index statements
                when (index) {
                    is SingleFieldIndex -> {
                        writer.writeln("db.${collectionResult.namespace.collection}.createIndex(${generateFields(listOf(index.field))}, ${generateOptions(
                            index.name,
                            index.sparse,
                            index.unique,
                            index.partialFilterExpression).toJson()});")
                    }
                    is CompoundIndex -> {
                        writer.writeln("db.${collectionResult.namespace.collection}.createIndex(${generateFields(index.fields)}, ${generateOptions(
                            index.name,
                            index.sparse,
                            index.unique,
                            index.partialFilterExpression).toJson()});")
                    }
                    is MultikeyIndex -> {
                        writer.writeln("db.${collectionResult.namespace.collection}.createIndex(${generateFields(index.fields)}, ${generateOptions(
                            index.name,
                            index.sparse,
                            index.unique,
                            index.partialFilterExpression).toJson()});")
                    }
                    is CompoundTextIndex -> {
                        writer.writeln("db.${collectionResult.namespace.collection}.createIndex(${generateCompoundTextFields(index.compoundFields, index.fields)}, ${generateTextOptions(
                            index.name,
                            index.sparse,
                            index.unique,
                            index.partialFilterExpression,
                            index.fields).toJson()});")
                    }
                    is TextIndex -> {
                        writer.writeln("db.${collectionResult.namespace.collection}.createIndex(${generateTextFields(index.fields)}, ${generateTextOptions(
                            index.name,
                            index.sparse,
                            index.unique,
                            index.partialFilterExpression,
                            index.fields).toJson()});")
                    }
                    is TwoDSphereIndex -> {
                        writer.writeln("db.${collectionResult.namespace.collection}.createIndex({ \"${index.key}\": \"2dsphere\" }, ${generateOptions(
                            index.name,
                            index.sparse,
                            index.unique,
                            index.partialFilterExpression).toJson()});")
                    }
                    is TwoDIndex -> {
                        writer.writeln("db.${collectionResult.namespace.collection}.createIndex({ \"${index.key}\": \"2d\" }, ${generateOptions(
                            index.name,
                            index.sparse,
                            index.unique,
                            index.partialFilterExpression).toJson()});")
                    }
                    is HashedIndex -> {
                        writer.writeln("db.${collectionResult.namespace.collection}.createIndex({ \"${index.field}\": \"hashed\" }, ${generateOptions(
                            index.name,
                            index.sparse,
                            index.unique,
                            index.partialFilterExpression).toJson()});")
                    }
                    is TTLIndex -> {
                        writer.writeln("db.${collectionResult.namespace.collection}.createIndex(${generateFields(listOf(index.field))}, ${generateOptions(
                            index.name,
                            index.sparse,
                            index.unique,
                            index.partialFilterExpression,
                            index.expireAfterSeconds).toJson()});")
                    }
                }
            }
        }

        writer.writeln()
    }

    private fun generateCompoundTextFields(compoundFields: List<Field>, fields: List<TextField>): BsonDocument {
        val document = BsonDocument()

        compoundFields.forEach { field ->
            document.append(field.name, BsonInt32(getDirection(field.direction)))
        }

        fields.forEach { field ->
            document.append(field.path.joinToString("."), BsonString("text"))
        }

        return document
    }

    private fun generateTextOptions(name: String, sparse: Boolean, unique: Boolean, partialFilterExpression: BsonDocument?, fields: List<TextField>): BsonDocument {
        val document = generateOptions(name, sparse, unique, partialFilterExpression)

        // Create weights document
        document.append("weights", BsonDocument(fields.map {
            BsonElement(it.path.joinToString("."), BsonInt32(it.weight))
        }))

        return document
    }

    private fun generateTextFields(fields: List<TextField>): String {
        return "{ ${fields.map { field ->
            """"${field.path.joinToString(".")}" : "text""""
        }.joinToString(", ")} }"
    }

    private fun generateOptions(name: String, sparse: Boolean, unique: Boolean, partialFilterExpression: BsonDocument?, expireAfterSeconds: Int = -1): BsonDocument {
        val document = BsonDocument()
            .append("name", BsonString(name))
            .append("unique", BsonBoolean(unique))
            .append("sparse", BsonBoolean(sparse))
            .append("background", BsonBoolean(true))

        if (partialFilterExpression != null) {
            document.append("partialFilterExpression", partialFilterExpression)
        }

        if (expireAfterSeconds >= 0) {
            document.append("expireAfterSeconds", BsonInt32(expireAfterSeconds))
        }

        return document
    }

    private fun generateFields(fields: List<Field>): String {
        return "{ ${fields.map { field ->
            """"${field.name}" : ${getDirection(field.direction)}"""
        }.joinToString(", ")} }"
    }

    private fun getDirection(direction: IndexDirection): Int {
        return when (direction) {
            IndexDirection.ASCENDING -> direction.value()
            IndexDirection.DESCENDING -> direction.value()
            IndexDirection.UNKNOWN -> IndexDirection.ASCENDING.value()
        }
    }
}