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

package com.mongodb.client;

import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

/**
 * The Mongo Cursor interface for change streams implementing the iterator protocol.
 * <p>
 * An application should ensure that a cursor is closed in all circumstances, e.g. using a try-with-resources statement:
 * </p>
 * <pre>{@code
 * try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch().cursor()) {
 *     while (cursor.hasNext()) {
 *         System.out.println(cursor.next());
 *     }
 * }
 * }</pre>
 *
 *
 * <p>
 * A {@link com.mongodb.MongoOperationTimeoutException} does not invalidate the {@link MongoChangeStreamCursor}, but is immediately
 * propagated to the caller. Subsequent method call will attempt to resume operation by establishing a new change stream on the server,
 * without doing {@code getMore} request first. </p>
 * <p>
 * If a {@link com.mongodb.MongoOperationTimeoutException} occurs before any events are received, it indicates that the server
 * has timed out before it could finish processing the existing oplog. In such cases, it is recommended to close the current stream
 * and recreate it with a higher timeout setting.
 *
 * @since 3.11
 * @param <TResult> The type of documents the cursor contains
 */
@NotThreadSafe
public interface MongoChangeStreamCursor<TResult> extends MongoCursor<TResult> {
    /**
     * Returns the resume token. If a batch has been iterated to the last change stream document in the batch
     * and a postBatchResumeToken is included in the document, the postBatchResumeToken will be returned.
     * Otherwise, the resume token contained in the last change stream document will be returned.
     *
     * @return the resume token, which can be null if the cursor has either not been iterated yet, or the cursor is closed.
     */
    @Nullable
    BsonDocument getResumeToken();
}
