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
import org.bson.BsonDocument;
import org.bson.BsonString;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.releasingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.withAsyncConnection;
import static com.mongodb.internal.operation.AsyncOperationHelper.writeConcernErrorTransformerAsync;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.SyncOperationHelper.executeCommand;
import static com.mongodb.internal.operation.SyncOperationHelper.withConnection;
import static com.mongodb.internal.operation.SyncOperationHelper.writeConcernErrorTransformer;

/**
 * Stops a running stream processor.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class StopStreamProcessorOperation implements WriteOperation<Void> {
    private static final String COMMAND_NAME = "stopStreamProcessor";
    private static final String DATABASE = "admin";

    private final String processorName;

    public StopStreamProcessorOperation(final String processorName) {
        this.processorName = notNull("processorName", processorName);
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
    public Void execute(final WriteBinding binding, final OperationContext operationContext) {
        return withConnection(binding, operationContext, (connection, operationContextWithMinRtt) -> {
            executeCommand(binding, operationContextWithMinRtt, DATABASE, getCommand(), connection,
                    writeConcernErrorTransformer(operationContextWithMinRtt.getTimeoutContext()));
            return null;
        });
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final OperationContext operationContext,
                             final SingleResultCallback<Void> callback) {
        withAsyncConnection(binding, operationContext, (connection, operationContextWithMinRtt, t) -> {
            SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
            if (t != null) {
                errHandlingCallback.onResult(null, t);
            } else {
                executeCommandAsync(binding, operationContextWithMinRtt, DATABASE, getCommand(), connection,
                        writeConcernErrorTransformerAsync(operationContextWithMinRtt.getTimeoutContext()),
                        releasingCallback(errHandlingCallback, connection));
            }
        });
    }

    private BsonDocument getCommand() {
        return new BsonDocument(COMMAND_NAME, new BsonString(processorName));
    }
}
