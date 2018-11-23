package com.mconsulting.indexrecommender.integration.log

import com.mconsulting.indexrecommender.log.LogParser
import com.mconsulting.indexrecommender.readResourceAsStream
import com.mconsulting.indexrecommender.readResourceAsURI
import org.junit.jupiter.api.Test
import java.io.BufferedReader
import java.io.File
import java.util.zip.ZipFile
import java.util.zip.ZipInputStream

class ParseLogFilesTest {
    @Test
    fun parseLogsFromMongo_3_4() {
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
        val parser = LogParser(stream)
        var index = 0

        while (parser.hasNext()) {
            try {
                val entry = parser.next()
                index += 1
            } catch (err: Exception) {
                println("[$index] - ${parser.line}")
                break
            }
        }

//        var line = stream.readLine()
//
//        do {
//            println(line)
//
//            line = stream.readLine()
//        } while (line != null)
    }
}