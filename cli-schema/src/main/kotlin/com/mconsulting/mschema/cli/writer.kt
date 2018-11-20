package com.mconsulting.mschema.cli

import java.io.Writer

class IndentationWriter(private val writer: Writer, private val indentSize: String = "  ") : Writer() {
    private var indentDepth = 0
    private var indent = false

    fun indent() = indentDepth++
    fun unIndent() = indentDepth--

    override fun write(cbuf: CharArray, off: Int, len: Int) {
        var index = off

        while (index < len) {
            when(cbuf[index]) {
                '\r' -> {
                    writer.write(cbuf[index].toInt())

                    // Skip any white space
                    if (index < len + 1 && cbuf[index + 1] == '\n') {
                        index++
                        writer.write(cbuf[index].toInt())
                    }

                    indent = true
                }
                '\n' -> {
                    writer.write(cbuf[index].toInt())
                    indent = true
                }
                else -> {
                    if (indent) {
                        indent = false
                        writer.write(indentSize.repeat(indentDepth))
                    }

                    writer.write(cbuf[index].toInt())
                }
            }

            index++
        }
    }

    override fun flush() {
        writer.flush()
    }

    override fun close() {
        writer.close()
    }

    override fun toString(): String {
        return writer.toString()
    }
}