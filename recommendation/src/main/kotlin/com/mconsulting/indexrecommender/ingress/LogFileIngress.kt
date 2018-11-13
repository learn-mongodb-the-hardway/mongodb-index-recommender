package com.mconsulting.indexrecommender.ingress

import com.mconsulting.indexrecommender.Namespace
import com.mconsulting.indexrecommender.log.LogParser
import java.io.BufferedReader
import java.io.File

class LogFileIngress(
    val logFile: File
) : Ingress {
    override fun forEach(namespaces: List<Namespace>, func: (value: Any) -> Unit) {
        if (!logFile.exists()) {
            throw Exception("log file ${logFile.absolutePath} not found")
        }

        // Crete a log parser
        val logParser = LogParser(BufferedReader(logFile.reader()))

        // Execute
        while (logParser.hasNext()) {
            func(logParser.next())
        }
    }
}