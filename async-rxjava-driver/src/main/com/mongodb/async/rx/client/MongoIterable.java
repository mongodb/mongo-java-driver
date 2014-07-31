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
     * @return an Observable representing the documents in the view.  It will send a notification for each document in the view before
     * completing.
     */
    Observable<T> forEach();
}
