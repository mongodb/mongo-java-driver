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

package com.mongodb.async;

import com.mongodb.Block;

/**
 * An asynchronous cursor of documents.
 *
 * @param <T> the document type
 *
 * @since 3.0
 */
public interface MongoAsyncCursor<T> {
    /**
     * Asynchronously iterate through the cursor results.
     *
     * @param block the block to execute for each document
     * @return A future that indicates when iteration is complete
     */
    MongoFuture<Void> forEach(Block<? super T> block);
}
