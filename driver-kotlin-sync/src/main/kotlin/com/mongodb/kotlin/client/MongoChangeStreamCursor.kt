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
package com.mongodb.kotlin.client

import com.mongodb.client.MongoChangeStreamCursor as JMongoChangeStreamCursor
import org.bson.BsonDocument

/**
 * The Mongo Cursor interface for change streams implementing the iterator protocol.
 *
 * An application should ensure that a cursor is closed in all circumstances, e.g. using a `use` statement:
 * ```
 *   collection.watch().cursor().use { c ->
 *      while (c.hasNext()) {
 *          println(c.next())
 *      }
 *  }
 * ```
 *
 * @param T The type of documents the cursor contains
 * @property wrapped the underlying sync cursor
 */
public class MongoChangeStreamCursor<T : Any>(private val wrapped: JMongoChangeStreamCursor<T>) :
    MongoCursor<T>(wrapped) {
    /**
     * Returns the resume token. If a batch has been iterated to the last change stream document in the batch and a
     * postBatchResumeToken is included in the document, the postBatchResumeToken will be returned. Otherwise, the
     * resume token contained in the last change stream document will be returned.
     *
     * @return the resume token, which can be null if the cursor has either not been iterated yet, or the cursor is
     *   closed.
     */
    public fun getResumeToken(): BsonDocument? = wrapped.resumeToken
}
