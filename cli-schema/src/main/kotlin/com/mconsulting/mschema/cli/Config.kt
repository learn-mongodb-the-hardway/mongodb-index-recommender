package com.mconsulting.mschema.cli

import com.mconsulting.mrelational.schema.extractor.Namespace
import com.mconsulting.mrelational.schema.extractor.OutputFormat
import com.mongodb.MongoClientURI
import com.mongodb.client.model.ValidationAction
import com.mongodb.client.model.ValidationLevel
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.InvalidArgumentException
import com.xenomachina.argparser.SystemExitException
import com.xenomachina.argparser.default
import java.io.File

class Config(parser: ArgParser) {
    val logging = LoggingConfig(parser)
    val general = GeneralConfig(parser)
    val extract = ExtractConfig(parser)
    val apply = ApplyConfig(parser)

    fun validate() {
        general.validate()

        // Only apply validations if extract has been specified
        if (general.extract) {
            extract.validate()
        }

        // Only apply validations if apply has been specified
        if (general.apply) {
            apply.validate()
        }
    }
}

data class FileNamespace(val db: String, val collection: String, val validationLevel: ValidationLevel, val validationAction: ValidationAction, val file: File)

class LoggingConfig(parser: ArgParser) {
    val quiet by parser.flagging("--quiet", help = "Logging: turn off all logging").default(false)
    val verbosity by parser.counting("-v", "--verbosity", help = "Logging: verbosity of logging (repeatable)")
        .default(0)
    val logPath by parser.storing("--logpath", help = "Logging: log file to send write to instead of stdout - has to be a file, not directory")
        .default<String?>(null)
}

class ApplyConfig(parser: ArgParser) {
    fun validate() {
        if (schemas.isEmpty()) {
            throw IllegalArgumentException("at least one --schema must be specified")
        }
    }

    val validationLevel by parser.storing("--validationLevel", help = """Apply: Specify the MongoDB Schema Validation Level. Must be one of [${ValidationLevel.STRICT}, ${ValidationLevel.MODERATE}]""") {
        try {
            // Validate if it's a valid string
            ValidationLevel.valueOf(this.toUpperCase())
        } catch (exception: IllegalArgumentException) {
            throw IllegalArgumentException("--validationLevel [$this] is not a valid MongoDB validation level. Please user one of [${ValidationLevel.STRICT}, ${ValidationLevel.MODERATE}]")
        }
    }.default(ValidationLevel.STRICT)

    val validationAction by parser.storing("--validationAction", help = """Apply: Specify the MongoDB Schema Validation Action. Must be one of [${ValidationAction.ERROR}, ${ValidationAction.WARN}]""") {
        try {
            // Validate if it's a valid string
            ValidationAction.valueOf(this.toUpperCase())
        } catch (exception: IllegalArgumentException) {
            throw IllegalArgumentException("--validationAction [$this] is not a valid MongoDB validation action. Please user one of [${ValidationAction.ERROR}, ${ValidationAction.WARN}]")
        }
    }.default(ValidationAction.ERROR)

    val schemas by parser.adding("--schema", help = "Apply: Specify a schema to apply, format <db.collection:file>, ex: [db1.coll:./quickstart_users_2018-10-18T09:03Z.json]") {
        val parts = this.split(":")

        if (parts.size < 2) {
            throw IllegalArgumentException("--schema must be of format <db.collection:file>, ex: [db1.coll:./quickstart_users_2018-10-18T09:03Z.json]")
        }

        // Get the file name
        val fileName = parts.subList(1, parts.size).joinToString(":").trim()
        // Second part must be an existing file
        val file = File(fileName)
        if (!file.exists() || !file.isFile) {
            throw IllegalArgumentException("--schema file at ${fileName} does not exist or is not a file")
        }

        val namespaceParts = parts.first().split(".")

        // Validate the namespace
        if (namespaceParts.size != 2) {
            throw IllegalArgumentException("--schema must be of format <db.collection:file>, ex: [db1.coll:./quickstart_users_2018-10-18T09:03Z.json]")
        }

        // Create a validation Level
        FileNamespace(namespaceParts.first(), namespaceParts.last(), validationLevel, validationAction, file)
    }
}

/**
 * Extract schemas example options
 *   --extract
 *   --uri mongodb://localhost:27017
 *   --namespace db1.users:1000     <db.collection:sampleSize (int = 0 means all)>
 *   --namespace db1.groups:1000
 *   --format (schema/json-schema-v4)
 *   --output-directory ./
 */
class GeneralConfig(parser: ArgParser) {
    fun validate() {
        if (!extract && !apply) {
            throw SystemExitException("either --extract or --apply option must be provided", 1)
        }

        if (extract && apply) {
            throw SystemExitException("either --extract or --apply option must be provided", 1)
        }
    }

    val version by parser.option<Unit>("--version", help = "General: display the version") {
        throw ShowVersionException("version      : ${App.version}${System.lineSeparator()}git revision : ${App.gitRev}")
    }.default(false)

    val extract by parser.flagging("--extract", help = "General: Extract schemas from MongoDB").default(false)
    val apply by parser.flagging("--apply", help = "General: Apply Validation Schemas to MongoDB").default(false)

    val uri by parser.storing("--uri", help = "Connection: MongoDB URI connection string [--uri mongodb://localhost:27017]") {
        MongoClientURI(this)
    }
}

class ExtractConfig(parser: ArgParser) {
    fun validate() {
        if (namespaces.isEmpty()) {
            throw IllegalArgumentException("at least one --namespace must be specified")
        }
    }

    val mergeDocuments by parser.flagging("--mergeDocuments", help = """Extract: Merge any document schemas into a single document schema""")
        .default(false)

    val outputFormat by parser.storing("--format", help = """Extract: Output format for schema extractor, one of ["schema", "mongodb-schema-v4"], ex: [--output schema]""") {
        when (this.toLowerCase()) {
            "schema" -> OutputFormat.SCHEMA
            "mongodb-schema-v4" -> OutputFormat.MONGODB_SCHEMA_V4
            else -> throw InvalidArgumentException("""Output format for schema extractor, one of ["schema", "json-schema-v4"], ex: [--output schema]""")
        }
    }.default(OutputFormat.SCHEMA)

    val outputDirectory by parser.storing("--output-directory", help = """Extract: Output directory for the extracted schemas, ex: [--output-directory ./]""") {
        val file = File(this)
        if (!file.isDirectory) {
            throw IllegalArgumentException("$this is not a valid directory path, directory not found")
        }

        file
    }.default(File("./"))

    val namespaces by parser.adding("--namespace", help = "Extract: Add a namespace to extract the schema from, format <db.collection:sampleSize (int = 0 means all)>, ex: [db1.coll:1000]") {
        val parts = this.split(":")

        if (parts.size != 2) {
            throw IllegalArgumentException("--namespace must be of format <db.collection:sampleSize (int = 0 means all)>, ex: [db1.coll:1000]")
        }

        // Second part must be an integer
        try {
            parts.last().toLong()
        } catch (ex: NumberFormatException) {
            throw IllegalArgumentException("--namespace must be of format <db.collection:sampleSize (int = 0 means all)>, ex: [db1.coll:1000]")
        }

        val namespaceParts = parts.first().split(".")

        // Validate the namespace
        if (namespaceParts.size != 2) {
            throw IllegalArgumentException("--namespace must be of format <db.collection:sampleSize (int = 0 means all)>, ex: [db1.coll:1000]")
        }

        Namespace(namespaceParts.first(), namespaceParts.last(), parts.last().toLong())
    }
}

class ShowVersionException(version: String) : SystemExitException(version, 0)
