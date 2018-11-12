package com.mconsulting.indexrecommender.log

import com.mconsulting.indexrecommender.readResourceAsReader
import org.junit.jupiter.api.Test
import java.io.BufferedReader

class LogParserTest {

    @Test
    fun parseQueryAndAggregationLogEntriesTest() {
        val reader = readResourceAsReader("logs/query_and_aggregation_log_4_0.txt")
        val logParser = LogParser(BufferedReader(reader))
        val logEntries = mutableListOf<LogEntry>()

        // Go over the log
        logParser.forEach {
            if (it is CommandLogEntry) {
                logEntries += it
            }
        }

        println()


//        while (logParser.hasNext()) {
//            logParser.next()
//        }
    }
}