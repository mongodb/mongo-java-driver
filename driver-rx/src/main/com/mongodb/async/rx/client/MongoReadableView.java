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

package com.mongodb.async.rx.client;

import rx.Observable;

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
     * @return an Observable representing the single document. If the view contains no documents,
     * the Observable will report completion. Otherwise it will send a single notification for the one document before completing.
     */
    Observable<T> one();

    /**
     * Count the number of documents in this view.
     *
     * @return an Observable representing the completion of the count. It will report exactly one event containing the count before
     * completing.
     */
    Observable<Long> count();
}
