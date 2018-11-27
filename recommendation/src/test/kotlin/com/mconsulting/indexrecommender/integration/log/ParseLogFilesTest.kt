package com.mconsulting.indexrecommender.integration.log

import com.mconsulting.indexrecommender.Integration
import com.mconsulting.indexrecommender.log.LogParser
import com.mconsulting.indexrecommender.log.LogParserOptions
import com.mconsulting.indexrecommender.readResourceAsStream
import com.mconsulting.indexrecommender.readResourceAsURI
import org.junit.jupiter.api.Test
import java.io.BufferedReader
import java.io.File
import java.util.zip.ZipFile
import java.util.zip.ZipInputStream

class ParseLogFilesTest {
    @Test
    @Integration
    fun parseLogsFromMongo_3_4() {
//        executeParse()
    }

    fun executeParse() {
        val file = "logs/full-logs/mongo-log-3_4.zip"
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
}