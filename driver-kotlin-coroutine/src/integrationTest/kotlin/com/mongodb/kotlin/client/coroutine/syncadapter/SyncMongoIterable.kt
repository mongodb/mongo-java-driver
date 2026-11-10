/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb.kotlin.client.coroutine.syncadapter

import com.mongodb.Function
import com.mongodb.client.MongoCursor
import com.mongodb.client.MongoIterable as JMongoIterable
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking

open class SyncMongoIterable<T>(private val delegate: Flow<T>) : JMongoIterable<T> {
    private var batchSize: Int? = null

    override fun iterator(): MongoCursor<T> = cursor()

    override fun cursor(): MongoCursor<T> = SyncMongoCursor(delegate)

    override fun first(): T? = runBlocking { delegate.firstOrNull() }

    override fun batchSize(batchSize: Int): SyncMongoIterable<T> = apply {
        this@SyncMongoIterable.batchSize = batchSize
    }

    @Suppress("UNCHECKED_CAST")
    override fun <A : MutableCollection<in T>?> into(target: A): A & Any {
        runBlocking { target?.addAll(delegate.toList()) }
        return target as (A & Any)
    }

    @Suppress("UNCHECKED_CAST")
    override fun <U : Any?> map(mapper: Function<T, U>): SyncMongoIterable<U> =
        SyncMongoIterable(delegate.map { mapper.apply(it as (T & Any)) as (U & Any) })
}
