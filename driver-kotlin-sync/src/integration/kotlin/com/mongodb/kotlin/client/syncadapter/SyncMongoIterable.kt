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
package com.mongodb.kotlin.client.syncadapter

import com.mongodb.Function
import com.mongodb.client.MongoCursor
import com.mongodb.client.MongoIterable as JMongoIterable
import com.mongodb.kotlin.client.MongoIterable

open class SyncMongoIterable<T>(val delegate: MongoIterable<T>) : JMongoIterable<T> {
    override fun iterator(): MongoCursor<T> = cursor()

    override fun cursor(): MongoCursor<T> = SyncMongoCursor(delegate.cursor())

    override fun first(): T? = delegate.firstOrNull()

    override fun batchSize(batchSize: Int): SyncMongoIterable<T> = apply { delegate.batchSize(batchSize) }

    @Suppress("UNCHECKED_CAST")
    override fun <A : MutableCollection<in T>?> into(target: A): A & Any {
        delegate.forEach { target?.add(it) }
        return target as (A & Any)
    }

    @Suppress("UNCHECKED_CAST")
    override fun <U : Any?> map(mapper: Function<T, U>): SyncMongoIterable<U> =
        SyncMongoIterable(
            delegate.map { it ->
                val i: T & Any = it as (T & Any)
                mapper.apply(i) as (U & Any)
            })
}
