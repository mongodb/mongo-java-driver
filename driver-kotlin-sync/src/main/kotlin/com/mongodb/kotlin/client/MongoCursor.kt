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
import com.mongodb.client.MongoChangeStreamCursor as JMongoChangeStreamCursor
import com.mongodb.client.MongoCursor as JMongoCursor
import java.io.Closeable
import org.bson.BsonDocument

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
 * @param T The type of documents the cursor contains
 */
public sealed interface MongoCursor<T : Any> : Iterator<T>, Closeable {

    /**
     * Gets the number of results available locally without blocking, which may be 0.
     *
     * If the cursor is known to be exhausted, returns 0. If the cursor is closed before it's been exhausted, it may
     * return a non-zero value.
     */
    public val available: Int

    /**
     * A special [next] case that returns the next element in the iteration if available or null.
     *
     * Tailable cursors are an example where this is useful. A call to [tryNext] may return null, ut in the future
     * calling [tryNext] would return a new element if a document had been added to the capped collection.</p>
     *
     * @return the next element in the iteration if available or null.
     * @see [Tailable Cursor](https://www.mongodb.com/docs/manual/reference/glossary/#term-tailable-cursor)
     */
    public fun tryNext(): T?

    /** @return the ServerCursor if available */
    public val serverCursor: ServerCursor?

    /** @return the ServerAddress */
    public val serverAddress: ServerAddress
}

/**
 * The Mongo Cursor interface for change streams implementing the iterator protocol.
 *
 * An application should ensure that a cursor is closed in all circumstances, e.g. using a `use` statement:
 * ```
 *   collection.watch().cursor().use { c ->
 *      while (c.hasNext()) {
 *          println(c.next())
 *      }
 *  }
 * ```
 *
 * @param T The type of documents the cursor contains
 */
public interface MongoChangeStreamCursor<T : Any> : MongoCursor<T> {
    /**
     * Returns the resume token. If a batch has been iterated to the last change stream document in the batch and a
     * postBatchResumeToken is included in the document, the postBatchResumeToken will be returned. Otherwise, the
     * resume token contained in the last change stream document will be returned.
     *
     * @return the resume token, which can be null if the cursor has either not been iterated yet, or the cursor is
     *   closed.
     */
    public val resumeToken: BsonDocument?
}

internal class MongoCursorImpl<T : Any>(private val wrapped: JMongoCursor<T>) : MongoCursor<T> {

    override fun hasNext(): Boolean = wrapped.hasNext()

    override fun next(): T = wrapped.next()

    override fun close(): Unit = wrapped.close()

    override val available: Int
        get() = wrapped.available()

    override fun tryNext(): T? = wrapped.tryNext()

    override val serverCursor: ServerCursor?
        get() = wrapped.serverCursor

    override val serverAddress: ServerAddress
        get() = wrapped.serverAddress
}

internal class MongoChangeStreamCursorImpl<T : Any>(private val wrapped: JMongoChangeStreamCursor<T>) :
    MongoChangeStreamCursor<T> {

    override fun hasNext(): Boolean = wrapped.hasNext()

    override fun next(): T = wrapped.next()

    override fun close(): Unit = wrapped.close()

    override val available: Int
        get() = wrapped.available()

    override fun tryNext(): T? = wrapped.tryNext()

    override val serverCursor: ServerCursor?
        get() = wrapped.serverCursor

    override val serverAddress: ServerAddress
        get() = wrapped.serverAddress

    override val resumeToken: BsonDocument?
        get() = wrapped.resumeToken
}
