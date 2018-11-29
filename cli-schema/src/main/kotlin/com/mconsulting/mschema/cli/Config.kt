package com.mconsulting.mschema.cli

import com.mconsulting.indexrecommender.Namespace
import com.mconsulting.indexrecommender.TimeResolution
import com.mongodb.MongoClientURI
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.InvalidArgumentException
import com.xenomachina.argparser.SystemExitException
import com.xenomachina.argparser.default
import java.io.File

class Config(parser: ArgParser) {
    val logging = LoggingConfig(parser)
    val general = GeneralConfig(parser)
    val extract = ExtractConfig(parser)
    val statistics = StatisticsConfig(parser)

    fun validate() {
        general.validate()
        extract.validate()
        statistics.validate()
    }
}

class LoggingConfig(parser: ArgParser) {
    val quiet by parser.flagging("--quiet", help = "Logging: turn off all logging").default(false)
    val verbosity by parser.counting("-v", "--verbosity", help = "Logging: verbosity of logging (repeatable)")
        .default(0)
    val logPath by parser.storing("--logpath", help = "Logging: log file to send write to instead of stdout - has to be a file, not directory")
        .default<String?>(null)
}

class StatisticsConfig(parser: ArgParser) {
    fun validate() {
    }

    val bucketResolution by parser.storing("--bucket-resolution", help = """Statistics: The statistics gathering resolution (millisecond, second, minute, hour, day)""") {
        when (this.toLowerCase()) {
            "txt" -> OutputFormat.TXT
            "json" -> OutputFormat.JSON
            else -> throw InvalidArgumentException("""Bucket resolution for statistics gathering must be one of ["millisecond", "second", "minute", "hour", "day"], ex: [--bucket-resolution minute]""")
        }
    }.default(TimeResolution.MINUTE)
}

class GeneralConfig(parser: ArgParser) {
    fun validate() {
    }

    val version by parser.option<Unit>("--version", help = "General: display the version") {
        throw ShowVersionException("version      : ${App.version}${System.lineSeparator()}git revision : ${App.gitRev}")
    }.default(false)

    val uri by parser.storing("--uri", help = "Connection: MongoDB URI connection string [--uri mongodb://localhost:27017]") {
        MongoClientURI(this)
    }
}

enum class OutputFormat {
    TXT, JSON
}

class ExtractConfig(parser: ArgParser) {
    fun validate() {
        if (namespaces.isEmpty()) {
            throw IllegalArgumentException("at least one --namespace must be specified")
        }
    }

    val skipReadProfileCollection by parser.flagging("--skip-read-profile-collection", help = "General: Skip attempting to queries the profile collection").default(false)

    val skipQueryShapeExplainPlanExecution by parser.flagging("--skip-queries-shape-explain-plan-execution", help = "General: Skip attempt to use detected queries shapes to detect additional information").default(false)

    val outputFormat by parser.storing("--format", help = """Extract: Output format for index recommender, one of ["txt", "json"], ex: [--output txt]""") {
        when (this.toLowerCase()) {
            "txt" -> OutputFormat.TXT
            "json" -> OutputFormat.JSON
            else -> throw InvalidArgumentException("""Output format for schema extractor, one of ["txt", "json"], ex: [--output txt]""")
        }
    }.default(OutputFormat.TXT)

    val outputDirectory by parser.storing("--output-directory", help = """Extract: Output directory for the index recommendations, ex: [--output-directory ./]""") {
        val file = File(this)
        if (!file.isDirectory) {
            throw IllegalArgumentException("$this is not a valid directory path, directory not found")
        }

        file
    }.default(File("-1"))

    val namespaces by parser.adding("--namespace", help = "Extract: Add a namespace to analyse indexes for, format <db.collection>, ex: [db1.coll]") {
        val namespaceParts = this.split(".")

        // Validate the namespace
        if (namespaceParts.size < 2) {
            throw IllegalArgumentException("--namespace must be of format <db.collection>, ex: [db1.coll]")
        }

        Namespace(namespaceParts.first(), namespaceParts.subList(1, namespaceParts.size).joinToString("."))
    }

    fun databaseNames() : List<String> {
        val dbNames = mutableSetOf<String>()

        namespaces.forEach {
            dbNames.add(it.db)
        }

        return dbNames.toList()
    }

    val mongoLogs by parser.adding("--mongolog", help = "Extract: One or more MongoDB logs to parse for analysis") {
        val file = File(this)
        if (!file.isFile) {
            throw IllegalArgumentException("$this log file not found")
        }

        file
    }
}

class ShowVersionException(version: String) : SystemExitException(version, 0)
