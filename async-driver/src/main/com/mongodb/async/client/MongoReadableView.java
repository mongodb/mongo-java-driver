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

import com.mongodb.async.MongoFuture;

/**
 * Asynchronous read operations on a collection view.
 *
 * @param <T> the document type
 * @since 3.0
 */
public interface MongoReadableView<T> {

    /**
     * Gets the first document in this view.  Which document returned is not deterministic unless a sort criteria has been applied to the
     * view.
     *
     * @return the first document in this view.
     */
    MongoFuture<T> one();

    /**
     * Count the number of documents in this view.
     *
     * @return the number of documents in this view.
     */
    MongoFuture<Long> count();
}
