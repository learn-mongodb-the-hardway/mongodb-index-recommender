package com.mconsulting.indexrecommender

class Namespace(val db: String, val collection: String) {
    companion object {
        fun parse(ns: String) : Namespace {
            val parts = ns.split(".")
            val db = parts.first().trim()
            val collection = parts.subList(1, parts.size).joinToString(".")
            return Namespace(db, collection)
        }
    }
}
