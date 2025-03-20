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

import com.mongodb.ServerAddress
import com.mongodb.ServerCursor
import com.mongodb.client.MongoCursor as JMongoCursor
import com.mongodb.kotlin.client.MongoCursor

internal open class SyncMongoCursor<T : Any>(private val delegate: MongoCursor<T>) : JMongoCursor<T> {
    override fun remove() {
        TODO("Not yet implemented")
    }

    override fun hasNext(): Boolean = delegate.hasNext()
    override fun next(): T = delegate.next()

    override fun close() = delegate.close()

    override fun available(): Int = delegate.available

    override fun tryNext(): T? = delegate.tryNext()

    override fun getServerCursor(): ServerCursor? = delegate.serverCursor

    override fun getServerAddress(): ServerAddress = delegate.serverAddress
}
