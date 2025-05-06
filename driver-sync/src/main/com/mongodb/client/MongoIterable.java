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

package com.mongodb.client;

import com.mongodb.Function;
import com.mongodb.lang.Nullable;

import java.util.Collection;

/**
 *The MongoIterable is the results from an operation, such as a query.
 *
 * @param <TResult> The type that this iterable will decode documents to.
 * @since 3.0
 */
public interface MongoIterable<TResult> extends Iterable<TResult> {

    /**
     * @return A {@link MongoCursor} that must be {@linkplain MongoCursor#close() closed}.
     */
    @Override
    MongoCursor<TResult> iterator();

    /**
     * Returns a cursor used for iterating over elements of type {@code TResult}. The cursor is primarily used for change streams.
     *
     * @return a cursor equivalent to that returned from {@link #iterator()}.
     * @since 3.11
     */
    MongoCursor<TResult> cursor();

    /**
     * Helper to return the first item in the iterator or null.
     *
     * @return T the first item or null.
     */
    @Nullable
    TResult first();

    /**
     * Maps this iterable from the source document type to the target document type.
     *
     * @param mapper a function that maps from the source to the target document type
     * @param <U> the target document type
     * @return an iterable which maps T to U
     */
    <U> MongoIterable<U> map(Function<TResult, U> mapper);

    /**
     * Iterates over all the documents, adding each to the given target.
     *
     * @param target the collection to insert into
     * @param <A> the collection type
     * @return the target
     */
    <A extends Collection<? super TResult>> A into(A target);

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    MongoIterable<TResult> batchSize(int batchSize);

}
