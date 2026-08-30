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
import com.mongodb.internal.MongoNamespaceHelper;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.releasingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.withAsyncConnection;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.SyncOperationHelper.executeCommand;
import static com.mongodb.internal.operation.SyncOperationHelper.withConnection;

/**
 * Opens a sample cursor on a running stream processor and returns the cursor ID.
 * The first batch of documents is fetched via {@link GetMoreSampleStreamProcessorOperation}.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class StartSampleStreamProcessorOperation implements WriteOperation<Long> {
    private static final String COMMAND_NAME = "startSampleStreamProcessor";
    private static final String DATABASE = "admin";
    private static final BsonDocumentCodec DECODER = new BsonDocumentCodec();

    private final String processorName;
    @Nullable
    private final Integer limit;

    public StartSampleStreamProcessorOperation(final String processorName, @Nullable final Integer limit) {
        this.processorName = notNull("processorName", processorName);
        this.limit = limit;
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }

    @Override
    public MongoNamespace getNamespace() {
        return MongoNamespaceHelper.ADMIN_DB_COMMAND_NAMESPACE;
    }

    @Override
    public Long execute(final WriteBinding binding, final OperationContext operationContext) {
        return withConnection(binding, operationContext, (connection, operationContextWithMinRtt) ->
                executeCommand(binding, operationContextWithMinRtt, DATABASE, getCommand(), DECODER,
                        (result, connection1) -> result.getInt64("cursorId").getValue()));
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final OperationContext operationContext,
                             final SingleResultCallback<Long> callback) {
        withAsyncConnection(binding, operationContext, (connection, operationContextWithMinRtt, t) -> {
            SingleResultCallback<Long> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
            if (t != null) {
                errHandlingCallback.onResult(null, t);
            } else {
                executeCommandAsync(binding, operationContextWithMinRtt, DATABASE, getCommand(), connection,
                        (result, connection1) -> result.getInt64("cursorId").getValue(),
                        releasingCallback(errHandlingCallback, connection));
            }
        });
    }

    private BsonDocument getCommand() {
        BsonDocument command = new BsonDocument(COMMAND_NAME, new BsonString(processorName));
        if (limit != null) {
            command.append("limit", new BsonInt32(limit));
        }
        return command;
    }
}
