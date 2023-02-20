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
package com.mongodb.kotlin.client

import com.mongodb.ServerAddress
import com.mongodb.ServerCursor
import com.mongodb.client.MongoCursor as JMongoCursor
import java.io.Closeable

/**
 * The Mongo Cursor interface implementing the iterator protocol.
 *
 * An application should ensure that a cursor is closed in all circumstances, e.g. using a `use` statement:
 * ```
 *  collection.find().cursor().use { c ->
 *      while (c.hasNext()) {
 *          println(c.next())
 *      }
 *  }
 * ```
 *
 * @param <T> The type of documents the cursor contains
 */
public open class MongoCursor<T>(private val wrapped: JMongoCursor<T>) : Iterator<T>, Closeable {

    public override fun hasNext(): Boolean = wrapped.hasNext()

    public override fun next(): T = wrapped.next()

    public override fun close(): Unit = wrapped.close()

    /**
     * Gets the number of results available locally without blocking, which may be 0.
     *
     * If the cursor is known to be exhausted, returns 0. If the cursor is closed before it's been exhausted, it may
     * return a non-zero value.
     *
     * @return the number of results available locally without blocking
     */
    public fun available(): Int = wrapped.available()

    /**
     * A special [next] case that returns the next element in the iteration if available or null.
     *
     * Tailable cursors are an example where this is useful. A call to [tryNext] may return null, ut in the future
     * calling [tryNext] would return a new element if a document had been added to the capped collection.</p>
     *
     * @return the next element in the iteration if available or null.
     * @see [Tailable Cursor](https://www.mongodb.com/docs/manual/reference/glossary/#term-tailable-cursor)
     */
    public fun tryNext(): T? = wrapped.tryNext()

    /** @return the ServerCursor if available */
    public fun getServerCursor(): ServerCursor? = wrapped.serverCursor

    /** @return the ServerAddress */
    public val serverAddress: ServerAddress
        get() = wrapped.serverAddress
}
