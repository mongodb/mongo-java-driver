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

import com.mongodb.MongoClientException
import com.mongodb.client.MongoIterable as JMongoIterable

/**
 * The MongoIterable is the results from an operation, such as a query.
 *
 * @param T The type that this iterable will decode documents to.
 */
public open class MongoIterable<T : Any>(private val delegate: JMongoIterable<T>) {

    /**
     * Returns a cursor used for iterating over elements of type `T. The cursor is primarily used for change streams.
     *
     * Note: Care must be taken to ensure the returned [MongoCursor] is closed after use.
     *
     * @return a cursor
     */
    public open fun cursor(): MongoCursor<T> = MongoCursorImpl(delegate.cursor())

    /** @return the first item or null */
    public fun firstOrNull(): T? = delegate.first()

    /** @return the first item or throw a [MongoClientException] if no results are available */
    public fun first(): T = firstOrNull() ?: throw MongoClientException("No results available")

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @see [Batch Size](https://www.mongodb.com/docs/manual/reference/method/cursor.batchSize/#cursor.batchSize)
     */
    public open fun batchSize(batchSize: Int): MongoIterable<T> = apply { delegate.batchSize(batchSize) }

    /**
     * Creates a new cursor and treats it as a [Sequence], invokes the given [consumer] function and closes the cursor
     * down correctly whether an exception is thrown or not.
     *
     * This allows the [MongoIterable] to be safely treated as a lazily evaluated sequence.
     *
     * Note: Sequence filters and aggregations have a performance cost, it is best to use server side filters and
     * aggregations where available.
     *
     * @param R the result type
     * @param consumer the sequence consumer
     * @return the result of the consumer
     */
    public fun <R> use(consumer: (Sequence<T>) -> R): R = cursor().use { consumer.invoke(it.asSequence()) }

    /**
     * Maps this iterable from the source document type to the target document type.
     *
     * @param R the result document type
     * @param transform a function that maps from the source to the target document type
     * @return an iterable which maps T to U
     */
    public fun <R : Any> map(transform: (T) -> R): MongoIterable<R> = MongoIterable(delegate.map(transform))

    /** Performs the given [action] on each element and safely closes the cursor. */
    public fun forEach(action: (T) -> Unit): Unit = use { it.forEach(action) }

    /**
     * Appends all elements to the given [destination] collection.
     *
     * @param C the type of the collection
     * @param destination the destination collection
     * @return the collection
     */
    public fun <C : MutableCollection<in T>> toCollection(destination: C): C = use { it.toCollection(destination) }

    /** @return a [List] containing all elements. */
    public fun toList(): List<T> = toCollection(ArrayList()).toList()
}
