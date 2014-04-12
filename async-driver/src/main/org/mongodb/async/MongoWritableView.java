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

package org.mongodb.async;

import org.mongodb.MongoFuture;
import org.mongodb.WriteResult;

/**
 * Writable operations on a collection view.
 *
 * @param <T> the document type
 * @since 3.0
 */
public interface MongoWritableView<T> {

    /**
     * Replace a document in the underlying collection with the given document, using the filter criteria of the view.
     *
     * @param replacement the replacement document
     * @return the result of the replacement
     */
    MongoFuture<WriteResult> replace(T replacement);
}
