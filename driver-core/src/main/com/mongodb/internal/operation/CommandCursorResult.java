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

package com.mongodb.internal.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;

/**
 * The command cursor result
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class CommandCursorResult<T> {

    private static final String CURSOR = "cursor";
    private static final String POST_BATCH_RESUME_TOKEN = "postBatchResumeToken";
    private static final String OPERATION_TIME = "operationTime";
    private final ServerAddress serverAddress;
    private final List<T> results;
    private final MongoNamespace namespace;
    private final long cursorId;
    @Nullable
    private final BsonTimestamp operationTime;
    @Nullable
    private final BsonDocument postBatchResumeToken;

    public CommandCursorResult(
            final ServerAddress serverAddress,
            final String fieldNameContainingBatch,
            final BsonDocument commandCursorDocument) {
        isTrue("Contains cursor", commandCursorDocument.isDocument(CURSOR));
        this.serverAddress = serverAddress;
        BsonDocument cursorDocument = commandCursorDocument.getDocument(CURSOR);
        this.results = BsonDocumentWrapperHelper.toList(cursorDocument, fieldNameContainingBatch);
        this.namespace = new MongoNamespace(cursorDocument.getString("ns").getValue());
        this.cursorId = cursorDocument.getNumber("id").longValue();
        this.operationTime = cursorDocument.getTimestamp(OPERATION_TIME, null);
        this.postBatchResumeToken = cursorDocument.getDocument(POST_BATCH_RESUME_TOKEN, null);
    }

    private CommandCursorResult(
            final ServerAddress serverAddress,
            final List<T> results,
            final MongoNamespace namespace,
            final long cursorId,
            @Nullable final BsonTimestamp operationTime,
            @Nullable final BsonDocument postBatchResumeToken) {
        this.serverAddress = serverAddress;
        this.results = results;
        this.namespace = namespace;
        this.cursorId = cursorId;
        this.operationTime = operationTime;
        this.postBatchResumeToken = postBatchResumeToken;
    }

    public static <T> CommandCursorResult<T> withEmptyResults(final CommandCursorResult<T> commandCursorResult) {
        return new CommandCursorResult<>(
                commandCursorResult.getServerAddress(),
                Collections.emptyList(),
                commandCursorResult.getNamespace(),
                commandCursorResult.getCursorId(),
                commandCursorResult.getOperationTime(),
                commandCursorResult.getPostBatchResumeToken());
    }

    /**
     * Gets the namespace.
     *
     * @return the namespace
     */
    public MongoNamespace getNamespace() {
        return namespace;
    }

    /**
     * Gets the cursor.
     *
     * @return the cursor, which may be null if it's been exhausted
     */
    @Nullable
    public ServerCursor getServerCursor() {
        return cursorId == 0 ? null : new ServerCursor(cursorId, serverAddress);
    }

    /**
     * Gets the results.
     *
     * @return the results
     */
    public List<T> getResults() {
        return results;
    }

    /**
     * Gets the server address.
     *
     * @return the server address
     */
    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    public long getCursorId() {
        return cursorId;
    }

    @Nullable
    public BsonDocument getPostBatchResumeToken() {
        return postBatchResumeToken;
    }

    @Nullable
    public BsonTimestamp getOperationTime() {
        return operationTime;
    }
}
