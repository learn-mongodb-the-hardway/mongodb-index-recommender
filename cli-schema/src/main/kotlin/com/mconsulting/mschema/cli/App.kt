package com.mconsulting.mschema.cli

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.turbo.TurboFilter
import ch.qos.logback.core.Appender
import ch.qos.logback.core.ConsoleAppender
import ch.qos.logback.core.FileAppender
import ch.qos.logback.core.encoder.Encoder
import ch.qos.logback.core.spi.FilterReply
import com.mconsulting.indexrecommender.CollectionOptions
import com.mconsulting.indexrecommender.IndexResults
import com.mconsulting.indexrecommender.Namespace
import com.mconsulting.indexrecommender.Processor
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
import com.mconsulting.indexrecommender.ingress.LogFileIngress
import com.mconsulting.indexrecommender.ingress.ProfileCollectionIngress
import com.mconsulting.indexrecommender.ingress.ProfileCollectionIngressOptions
import com.mongodb.MongoClient
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.HelpFormatter
import com.xenomachina.argparser.ShowHelpException
import com.xenomachina.argparser.SystemExitException
import mu.KLogging
import org.bson.BsonDocument
import org.slf4j.Marker
import java.io.File
import java.io.FileWriter
import java.io.OutputStreamWriter
import java.io.StringWriter
import java.io.Writer
import java.text.SimpleDateFormat
import java.util.*

fun Date.toISO8601() : String {
    val tz = TimeZone.getTimeZone("UTC");
    val df = SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
    df.setTimeZone(tz);
    return df.format(this);
}

object App : KLogging() {
    val dateFormat = SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")

    private val props by lazy {
        Properties().apply {
            load(Config::class.java.classLoader.getResourceAsStream("config.properties"))
        }
    }

    val name by lazy {
        props["name"]?.toString() ?: "unknown"
    }

    val gitRev by lazy {
        props["gitRev"]?.toString() ?: "unknown"
    }
    val version by lazy {
        props["version"]?.toString() ?: "unknown"
    }

    fun execute(args: Array<String>) = execute {
        if (args.isEmpty()) {
            loadConfig(arrayOf("--help"))
        }

        // Parse the options
        val config = loadConfig(args)
        // Attempt to validate the options
        config.validate()

        logger.info("version      : ${App.version}")
        logger.info("git revision : ${App.gitRev}")

        // Connect to MongoDB
        val client = MongoClient(config.general.uri)

        // Run recommendation engine
        executeRecommendation(client, config)

        // Close MongoDB Connection
        client.close()
    }

    private fun executeRecommendation(client: MongoClient, config: Config) {
        // Create processor
        val processor = Processor(client, config.extract.namespaces, CollectionOptions(
            allowExplainExecution = !config.extract.skipQueryShapeExplainPlanExecution
        ))

        // Add profile collection ingress sources
        if (!config.extract.skipReadProfileCollection) {
            config.extract.databaseNames().forEach {
                processor.addSource(ProfileCollectionIngress(client, Namespace(it, ""), ProfileCollectionIngressOptions(
                    quiet = config.logging.quiet
                )))
            }
        }

        // Add any log files
        config.extract.mongoLogs.forEach {
            processor.addSource(LogFileIngress(it))
        }

        // Execute the processor
        val indexResults = processor.process()

        // Create the output
        when (config.extract.outputFormat) {
            OutputFormat.TXT -> outputTextFormat(indexResults, config)
            OutputFormat.JSON -> outputJSONFormat(indexResults, config)
        }
    }

    private fun outputJSONFormat(indexResults: IndexResults, config: Config) {
    }

    private fun outputTextFormat(indexResults: IndexResults, config: Config) {
        indexResults.dbIndexResults.forEach { db ->
            val stringWriter = StringWriter()
            val writer = when(config.extract.outputDirectory.name) {
                "-1" -> IndentationWriter(stringWriter)
                else -> IndentationWriter(FileWriter(File(config.extract.outputDirectory, "${db.namespace.db}.txt")))
            }

            writer.writeln("db: ${db.namespace.db}")
            writer.writeln()
            writer.indent()

            db.collectionIndexResults.forEach { collection ->
                writer.writeln("collection: ${collection.namespace.collection}")
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
                    if (index.indexStatistics != null) {
                        writer.writeln("count: ${index.indexStatistics!!.ops}")
                        writer.writeln("since: ${index.indexStatistics!!.since}")
                    } else {
                        writer.writeln("count: ${index.statistics.map { it.count }.sum()}")
                    }

                    writer.unIndent()

                    writer.unIndent()
                    writer.writeln()
                }

                writer.unIndent()
            }

            writer.flush()
            writer.close()

            if (config.extract.outputDirectory.name == "-1") {
                println(stringWriter.buffer.toString())
            }
        }
    }

    private fun writeIndexSpecific(writer: IndentationWriter, index: Index) {
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

    private fun indexTypeName(index: Index): String {
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

    private fun execute(body: () -> Unit) {
        try {
            body()
        } catch (e: ShowHelpException) {
            e.printAndExit(name, 80)
        } catch (e: ShowVersionException) {
            e.printAndExit(null, 80)
        } catch (e: SystemExitException) {
            val writer = OutputStreamWriter(System.err)
            e.printUserMessage(writer, name, 80)
            writer.writeln()
            writer.write("To see all the options, start: $name --help")
            writer.flush()
        } finally {
        }
    }

    private fun loadConfig(args: Array<String>): Config {
        val parser = ArgParser(args, helpFormatter = AppHelpFormatter)
        val config = Config(parser)
        config.validate()

        configureLogging(config.logging)

        return config
    }

    private fun configureLogging(logging: LoggingConfig) {
        val loggerContext = org.slf4j.LoggerFactory.getILoggerFactory() as LoggerContext
        val logger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME)

        logger.detachAppender("console")

        val encoder = PatternLayoutEncoder()
        encoder.pattern = "%date %level %logger: %msg%n"
        encoder.context = loggerContext
        encoder.start()

        val appender = if (logging.logPath != null) {
            createFileAppender(encoder, loggerContext, logging)
        } else {
            createConsoleAppender(encoder, loggerContext, logging)
        }

        logger.addAppender(appender)

        logger.level = when (logging.verbosity) {
            0 -> Level.INFO
            1 -> Level.DEBUG
            else -> Level.ALL
        }

        if (logging.quiet) {
            logger.level = Level.OFF
        }

        loggerContext.addTurboFilter(object : TurboFilter() {
            override fun decide(marker: Marker?, logger: ch.qos.logback.classic.Logger?, level: Level?, format: String?, params: Array<out Any>?, t: Throwable?): FilterReply {
                return if (logger?.name?.startsWith("com.mconsulting.mschema") == false) {
                    FilterReply.DENY
                } else FilterReply.NEUTRAL
            }
        })
    }

    private fun createConsoleAppender(encoder: Encoder<ILoggingEvent>, loggerContext: LoggerContext, logging: LoggingConfig): Appender<ILoggingEvent> {
        val appender = ConsoleAppender<ILoggingEvent>()
        appender.target = "System.err"
        appender.encoder = encoder
        appender.context = loggerContext
        appender.start()

        return appender
    }

    private fun createFileAppender(encoder: Encoder<ILoggingEvent>, loggerContext: LoggerContext, logging: LoggingConfig): Appender<ILoggingEvent> {
        val appender = FileAppender<ILoggingEvent>()
        appender.file = logging.logPath
        appender.encoder = encoder
        appender.context = loggerContext
        appender.start()

        return appender
    }
}

internal object AppHelpFormatter : HelpFormatter {
    override fun format(progName: String?, columns: Int, values: List<HelpFormatter.Value>): String {
        val builder = StringBuilder()

        builder.appendln("usage: $progName [OPTIONS]")

        values
            .filter { !it.isPositional }
            .map { value ->
                val idx = value.help.indexOf(':')
                val category = if (idx == -1) "General" else value.help.substring(0, idx)
                val help = if (idx == -1) value.help else value.help.substring(idx + 2)
                Triple(category, help, value)
            }
            .groupBy { it.first }
            .filter { it.key != "hidden" }
            .forEach { group ->
                builder.appendln()
                builder.appendln(group.key + " Options:")
                group.value.sortedBy { it.third.usages[0].trim('-') }.forEach { (_, second, third) ->
                    builder.appendln(third.usages.joinToString { it })
                    builder.appendln("\t\t$second")
                }
            }

        return builder.toString()
    }
}

fun Writer.writeln() {
    write(System.lineSeparator())
}

fun Writer.writeln(text: String) {
    write(text)
    write(System.lineSeparator())
}
