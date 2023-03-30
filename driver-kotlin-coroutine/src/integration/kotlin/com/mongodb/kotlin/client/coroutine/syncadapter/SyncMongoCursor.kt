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

import com.mongodb.ServerAddress
import com.mongodb.ServerCursor
import com.mongodb.client.MongoCursor
import java.lang.UnsupportedOperationException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking

open class SyncMongoCursor<T>(private val delegate: Flow<T>) : MongoCursor<T> {

    val iterator: Iterator<T> by lazy { runBlocking { delegate.toList() }.iterator() }

    override fun remove() {
        TODO("Not yet implemented")
    }

    override fun hasNext(): Boolean = iterator.hasNext()
    @Suppress("UNCHECKED_CAST") override fun next(): T & Any = iterator.next() as (T & Any)

    override fun close() {}

    override fun available(): Int = throw UnsupportedOperationException()

    override fun tryNext(): T? = throw UnsupportedOperationException()

    override fun getServerCursor(): ServerCursor? = throw UnsupportedOperationException()

    override fun getServerAddress(): ServerAddress = throw UnsupportedOperationException()
}
