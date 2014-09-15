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

import com.mongodb.annotations.NotThreadSafe;
import org.mongodb.ConvertibleToDocument;
import org.mongodb.Document;

/**
 * A view onto a collection.  Operations on the view will change which, how many, and in what order the documents appear in the view when
 * the contents of the view are accessed.
 * <p>
 * If a method specifying a property of this view is called multiple times, the last invocation wins.
 * </p>
 * <p>
 * Instances of this class are mutable and not thread-safe.
 * </p>
 *
 * @param <T> the document type
 * @since 3.0
 */
@NotThreadSafe
public interface MongoView<T> extends MongoWritableView<T>, MongoReadableView<T>, MongoIterable<T> {

    /**
     * Updates the filter applied to the documents in the view.
     *
     * @param filter the filter
     * @return this
     */
    MongoView<T> find(Document filter);

    /**
     * Updates the filter applied to the documents in the view.
     *
     * @param filter the filter
     * @return this
     */
    MongoView<T> find(ConvertibleToDocument filter);


    /**
     * The sort criteria for documents in the view.
     *
     * @param sortCriteria the sort criteria
     * @return this
     */
    MongoView<T> sort(Document sortCriteria);

    /**
     * The sort criteria for documents in the view.
     *
     * @param sortCriteria the sort criteria
     * @return this
     */
    MongoView<T> sort(ConvertibleToDocument sortCriteria);

    /**
     * Specifies the number of documents to skip in the underlying collection.
     *
     * @param skip the number of documents to skip
     * @return this
     */
    MongoView<T> skip(int skip);

    /**
     * Specifies the limit on the number of documents in the view of the underlying collection.
     *
     * @param limit the limit on the number of documents
     * @return this
     */
    MongoView<T> limit(int limit);

    /**
     * Specifies the fields to select in documents in the view of the underlying collection.
     *
     * @param selector the fields to select
     * @return this
     */
    MongoView<T> fields(Document selector);

    /**
     * Specifies the fields to select in documents in the view of the underlying collection.
     *
     * @param selector the fields to select
     * @return this
     */
    MongoView<T> fields(ConvertibleToDocument selector);

    /**
     * Specifies that update operations executed on this view will result in inserts if no document with the _id of the document exists
     * in the collection.
     *
     * @return this
     */
    MongoView<T> upsert();
}
