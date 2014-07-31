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

import org.mongodb.Document;
import org.mongodb.WriteResult;
import rx.Observable;

/**
 * Writable operations on a collection view.
 *
 * @param <T> the document type
 * @since 3.0
 */
public interface MongoWritableView<T> {

    /**
     * With the given document, replace a single document in the underlying collection matching the filter criteria in the view.
     *
     * @param replacement the replacement document
     * @return an Observable representing the completion of the replace. It will report exactly one event when the command completes
     * successfully.
     */
    Observable<WriteResult> replace(T replacement);


    /**
     * With the given update operations, update all documents in the underlying collection matching the filter criteria in the view.
     *
     * @param updateOperations the update operations to apply to each document
     * @return an Observable representing the completion of the update. It will report exactly one event when the command completes
     * successfully.
     */
    Observable<WriteResult> update(Document updateOperations);

    /**
     /**
     * With the given update operations, update a single document in the underlying collection matching the filter criteria in the view.
     *
     * @param updateOperations the update operations to apply to each document
     * @return an Observable representing the completion of the update. It will report exactly one event when the command completes
     * successfully.
     */
    Observable<WriteResult> updateOne(Document updateOperations);

    /**
     * Removes all the documents in the underlying collection matching the filter criteria in the view.
     *
     * @return an Observable representing the completion of the update. It will report exactly one event when the command completes
     * successfully.
     */
    Observable<WriteResult> remove();

    /**
     * Removes one document in the underlying collection matching the filter criteria in the view.
     *
     * @return an Observable representing the completion of the update. It will report exactly one event when the command completes
     * successfully.
     */
    Observable<WriteResult> removeOne();
}
