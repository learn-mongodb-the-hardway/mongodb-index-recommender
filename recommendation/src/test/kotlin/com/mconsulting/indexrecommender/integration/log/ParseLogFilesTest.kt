package com.mconsulting.indexrecommender.integration.log

import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
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
import org.junit.jupiter.api.Test
import java.io.BufferedReader
import java.io.File
import java.io.StringReader
import java.util.zip.ZipFile
import java.util.zip.ZipInputStream

class ParseLogFilesTest {
    @Test
    @Integration
    fun parseLogsFromMongo_3_4() {
        executeParse("logs/full-logs/mongo-log-3_4.zip")
    }

    @Test
    @Integration
    fun parseLogsFromMongo_4_0() {
        executeParse("logs/full-logs/mongo-log-4_0.zip")
    }

    @Test
    @Integration
    fun parseProfileCollectionDumpFromMongo_3_4() {
        executeProfileParse("logs/full-logs/mongo-log-3_4.zip")
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
        class BufferJSONIngress(val bufferedReader: BufferedReader) : Ingress {
            override fun forEach(namespaces: List<Namespace>, func: (value: Any) -> Unit) {
                while (true) {
                    // Read the line
                    val line = bufferedReader.readLine() ?: break
                    println(line)
                    // Parse the json
                    val doc = Parser().parse(StringReader(line)) as JsonObject
                    // Create operation
                    val operation = createOperation(doc)
                    // Call the function
                    func(operation!!)
                }
            }
        }

        // Create a processor
        val processor = Processor(client)
        // Add the Ingres method
        processor.addSource(BufferJSONIngress(bufferedReader))
        // Run the processor
        val results = processor.process()
        println()
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
//            try {
                val entry = parser.next()
//            } catch (err: Exception) {
//                failedIndex += 1
//                println(err.stackTrace.toString())
//                println("[$index] - ${parser.line}")
//                throw err
//            }

//            if (index == 10000) {
//                break
//            }

            index += 1
        }

        println("========= total statements: $index")
        println("========= total failed statements: $failedIndex")

//        var line = stream.readLine()
//
//        do {
//            println(line)
//
//            line = stream.readLine()
//        } while (line != null)
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