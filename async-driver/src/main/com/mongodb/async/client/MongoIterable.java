/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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
import com.mongodb.async.MongoFuture;
import org.mongodb.Function;

import java.util.Collection;

/**
 * Operations that allow asynchronous iteration over a collection view.
 *
 * @param <T> the document type
 * @since 3.0
 */
public interface MongoIterable<T> {

    /**
     * Iterates over all documents in the view, applying the given block to each, and completing the returned future after all documents
     * have been iterated, or an exception has occurred.
     *
     * @param block the block to apply to each document
     * @return a future indicating when iteration is complete
     */
    MongoFuture<Void> forEach(Block<? super T> block);

    /**
     * Iterates over all the documents, adding each to the given target.
     *
     * @param target the collection to insert into
     * @param <A> the collection type
     * @return a future which will after all documents have been added to target
     */
    <A extends Collection<? super T>> MongoFuture<A> into(A target);

    /**
     * Maps this iterable from the source document type to the target document type.
     *
     * @param mapper a function that maps from the source to the target document type
     * @param <U> the target document type
     * @return an iterable which maps T to U
     */
    <U> MongoIterable<U> map(Function<T, U> mapper);
}
