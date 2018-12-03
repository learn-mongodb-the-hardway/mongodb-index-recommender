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
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.io.BufferedReader
import java.io.File
import java.io.StringReader
import java.lang.Exception
import java.util.zip.ZipFile
import java.util.zip.ZipInputStream

class ParseLogFilesTest {
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

    private fun executeProfileParse(file: String) {
        val inputStream = readResourceAsStream(file)
        val zipInputStream = ZipInputStream(inputStream)
        var entry = zipInputStream.nextEntry
        var zipFile = ZipFile(File(readResourceAsURI(file)))

        do {
            if (entry.name.endsWith(".json")) {
                parseJson(zipFile.getInputStream(entry).bufferedReader())
//                parseJson(BufferedReader(StringReader("""{"op":"command","ns":"test","command":{"${'$'}eval":{"${'$'}code":"function () {\n        return 33;\n    }"}},"numYield":0,"locks":{"Global":{"acquireCount":{"r":{"${'$'}numberLong":"3"},"W":{"${'$'}numberLong":"1"}}},"Database":{"acquireCount":{"r":{"${'$'}numberLong":"1"}}},"Collection":{"acquireCount":{"r":{"${'$'}numberLong":"1"}}}},"responseLength":38,"protocol":"op_command","millis":23,"ts":{"${'$'}date":"2018-11-22T16:00:09.406Z"},"client":"127.0.0.1","appName":"MongoDB Shell","allUsers":[],"user":""}""".trimIndent())))
            }
            entry = zipInputStream.nextEntry
        } while (entry != null)

        zipFile.close()
        inputStream.close()
    }

    private fun parseJson(bufferedReader: BufferedReader) {
        var index = 0

        class BufferJSONIngress(val bufferedReader: BufferedReader) : Ingress {
            override fun forEach(namespaces: List<Namespace>, func: (value: Any) -> Unit) {
                while (true) {
                    val originalLine = bufferedReader.readLine() ?: break
                    // Read the line
                    var line = originalLine
                    line = line
                        .replace("-Infinity", "-9007199254740991")
                        .replace("+Infinity", "9007199254740991")
                        .replace("NaN", "\"NaN\"")
                    // 24691
                    // 231134
//                        .replace(" -Infinity", " -9007199254740991")
//                        .replace(" +Infinity", " 9007199254740991")
//                        .replace(" NaN", " \"NaN\"")
//                    println(line)
                    // Parse the json
                    try {
                        val doc = Parser().parse(StringReader(line)) as JsonObject
                        // Create operation
                        val operation = createOperation(doc)
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
//            } catch (err: FailedOperation) {
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