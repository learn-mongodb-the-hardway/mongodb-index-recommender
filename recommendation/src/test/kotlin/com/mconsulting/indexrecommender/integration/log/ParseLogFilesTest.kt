package com.mconsulting.indexrecommender.integration.log

import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import com.mconsulting.indexrecommender.CollectionOptions
import com.mconsulting.indexrecommender.Integration
import com.mconsulting.indexrecommender.Namespace
import com.mconsulting.indexrecommender.Processor
import com.mconsulting.indexrecommender.createOperation
import com.mconsulting.indexrecommender.ingress.Ingress
import com.mconsulting.indexrecommender.log.LogParser
import com.mconsulting.indexrecommender.log.LogParserOptions
import com.mconsulting.indexrecommender.readResourceAsStream
import com.mconsulting.indexrecommender.readResourceAsURI
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoDatabase
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.io.BufferedReader
import java.io.File
import java.io.StringReader
import java.lang.Exception
import java.util.zip.ZipFile
import java.util.zip.ZipInputStream

class ParseLogFilesTest {
    val quiet = false

    @Test
    @Tag("integration")
    @Integration
    fun parseLogsFromMongo_3_4() {
        executeParse("logs/full-logs/mongo-log-3_4.zip")
    }

    @Test
    @Tag("integration")
    @Integration
    fun parseLogsFromMongo_4_0() {
        executeParse("logs/full-logs/mongo-log-4_0.zip")
    }

    @Test
    @Tag("integration")
    @Integration
    fun parseProfileCollectionDumpFromMongo_3_4() {
        executeProfileParse("logs/full-logs/mongo-log-3_4.zip")
    }

    @Test
    @Tag("integration")
    @Integration
    fun parseProfileCollectionDumpFromMongo_4_0() {
        executeProfileParse("logs/full-logs/mongo-log-4_0.zip")
    }

    private fun executeProfileParse(file: String) {
        val inputStream = readResourceAsStream(file)
        val zipInputStream = ZipInputStream(inputStream)
        var entry = zipInputStream.nextEntry
        var zipFile = ZipFile(File(readResourceAsURI(file)))

        do {
            if (entry.name.endsWith(".json")) {
                parseJson(zipFile.getInputStream(entry).bufferedReader())
            }
            entry = zipInputStream.nextEntry
        } while (entry != null)

        zipFile.close()
        inputStream.close()
    }

    private fun parseJson(bufferedReader: BufferedReader) {
        var index = 0

        class BufferJSONIngress(val reader: BufferedReader) : Ingress {
            override fun forEach(namespaces: List<Namespace>, func: (value: Any) -> Unit) {
                while (true) {
                    val originalLine = reader.readLine() ?: break
                    // Read the line
                    var line = originalLine
                    line = line
                        .replace("-Infinity", "-9007199254740991")
                        .replace("+Infinity", "9007199254740991")
                        .replace("NaN", "\"NaN\"")
                        .replace("\"\"NaN\"\"", "\"NaN\"")
                    // Parse the json
                    try {
                        val doc = Parser().parse(StringReader(line)) as JsonObject
                        // Create operation
                        val operation = createOperation(doc, quiet)
                        // Call the function
                        func(operation!!)
                    } catch (err: Exception) {
                        println("[$index]$originalLine")
                        throw err
                    }

                    index += 1
                }
            }
        }

        // Create a processor
        val processor = Processor(client, mutableListOf(), CollectionOptions(quiet = quiet))
        // Add the Ingres method
        processor.addSource(BufferJSONIngress(bufferedReader))
        // Run the processor
        processor.process()
    }

    fun executeParse(file: String) {
        val inputStream = readResourceAsStream(file)
        val zipInputStream = ZipInputStream(inputStream)
        var entry = zipInputStream.nextEntry
        var zipFile = ZipFile(File(readResourceAsURI(file)))

        do {
            if (entry.name.endsWith(".txt")) {
                parseLog(zipFile.getInputStream(entry).bufferedReader())
            }

            entry = zipInputStream.nextEntry
        } while (entry != null)

        zipFile.close()
        inputStream.close()
    }

    private fun parseLog(stream: BufferedReader) {
        val parser = LogParser(stream, LogParserOptions(true))
        var index = 0
        var failedIndex = 0

        while (parser.hasNext()) {
            parser.next()
            index += 1
        }

        println("========= total statements: $index")
        println("========= total failed statements: $failedIndex")
    }

    companion object {
        lateinit var client: MongoClient
        lateinit var db: MongoDatabase

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            client = MongoClient(MongoClientURI("mongodb://localhost:27017"))
            db = client.getDatabase("mindex_recommendation_tests")

            db.getCollection("users").drop()
            db.getCollection("games").drop()
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }
}