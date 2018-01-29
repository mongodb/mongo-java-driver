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

package com.mongodb.async.client;

import com.mongodb.Block;
import com.mongodb.Function;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;

import java.util.Collection;

/**
 * Operations that allow asynchronous iteration over a collection view.
 *
 * @param <TResult> the result type
 * @since 3.0
 */
public interface MongoIterable<TResult> {

    /**
     * Helper to return the first item in the iterator or null.
     *
     * @param callback a callback that is passed the first item or null.
     */
    void first(SingleResultCallback<TResult> callback);

    /**
     * Iterates over all documents in the view, applying the given block to each, and completing the returned future after all documents
     * have been iterated, or an exception has occurred.
     *
     * @param block    the block to apply to each document
     * @param callback a callback that completed once the iteration has completed
     */
    void forEach(Block<? super TResult> block, SingleResultCallback<Void> callback);

    /**
     * Iterates over all the documents, adding each to the given target.
     *
     * @param target   the collection to insert into
     * @param <A>      the collection type
     * @param callback a callback that will be passed the target containing all documents
     */
    <A extends Collection<? super TResult>> void into(A target, SingleResultCallback<A> callback);

    /**
     * Maps this iterable from the source document type to the target document type.
     *
     * @param mapper a function that maps from the source to the target document type
     * @param <U>    the target document type
     * @return an iterable which maps T to U
     */
    <U> MongoIterable<U> map(Function<TResult, U> mapper);

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    MongoIterable<TResult> batchSize(int batchSize);

    /**
     * Gets the number of documents to return per batch or null if not set.
     *
     * @return the batch size, which may be null
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     * @since 3.7
     */
    Integer getBatchSize();

    /**
     * Provide the underlying {@link com.mongodb.async.AsyncBatchCursor} allowing fine grained control of the cursor.
     *
     * @param callback a callback that will be passed the AsyncBatchCursor
     */
    void batchCursor(SingleResultCallback<AsyncBatchCursor<TResult>> callback);
}
