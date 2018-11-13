package com.mconsulting.indexrecommender.ingress

import com.mconsulting.indexrecommender.Namespace

interface Ingress {
    fun forEach(namespaces: List<Namespace>, func: (value: Any) -> Unit)
}