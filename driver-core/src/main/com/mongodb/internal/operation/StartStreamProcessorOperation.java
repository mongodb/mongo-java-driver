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
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;

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
 * Starts a stream processor. The processor must be in the {@code STOPPED} or {@code FAILED} state.
 *
 * <p>NOTE: {@code startAfter} is reserved for future use and is intentionally never serialized to the wire.</p>
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class StartStreamProcessorOperation implements WriteOperation<Void> {
    private static final String COMMAND_NAME = "startStreamProcessor";
    private static final String DATABASE = "admin";

    private final String processorName;
    @Nullable
    private final Integer workers;
    @Nullable
    private final Boolean clearCheckpoints;
    @Nullable
    private final BsonTimestamp startAtOperationTime;
    @Nullable
    private final String tier;
    @Nullable
    private final Boolean enableAutoScaling;
    @Nullable
    private final String failoverRegion;
    @Nullable
    private final String failoverMode;
    @Nullable
    private final Boolean failoverDryRun;

    public StartStreamProcessorOperation(final String processorName,
                                         @Nullable final Integer workers,
                                         @Nullable final Boolean clearCheckpoints,
                                         @Nullable final BsonTimestamp startAtOperationTime,
                                         @Nullable final String tier,
                                         @Nullable final Boolean enableAutoScaling,
                                         @Nullable final String failoverRegion,
                                         @Nullable final String failoverMode,
                                         @Nullable final Boolean failoverDryRun) {
        this.processorName = notNull("processorName", processorName);
        this.workers = workers;
        this.clearCheckpoints = clearCheckpoints;
        this.startAtOperationTime = startAtOperationTime;
        this.tier = tier;
        this.enableAutoScaling = enableAutoScaling;
        this.failoverRegion = failoverRegion;
        this.failoverMode = failoverMode;
        this.failoverDryRun = failoverDryRun;
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
        BsonDocument command = new BsonDocument(COMMAND_NAME, new BsonString(processorName));

        if (workers != null) {
            command.append("workers", new BsonInt32(workers));
        }

        BsonDocument options = new BsonDocument();
        if (clearCheckpoints != null) {
            options.append("clearCheckpoints", BsonBoolean.valueOf(clearCheckpoints));
        }
        if (startAtOperationTime != null) {
            options.append("startAtOperationTime", startAtOperationTime);
        }
        // NOTE: startAfter is RESERVED and MUST NOT be sent to the server.
        if (tier != null) {
            options.append("tier", new BsonString(tier));
        }
        if (enableAutoScaling != null) {
            options.append("enableAutoScaling", BsonBoolean.valueOf(enableAutoScaling));
        }
        if (!options.isEmpty()) {
            command.append("options", options);
        }

        if (failoverRegion != null) {
            BsonDocument failover = new BsonDocument("region", new BsonString(failoverRegion));
            if (failoverMode != null) {
                failover.append("mode", new BsonString(failoverMode));
            }
            if (failoverDryRun != null) {
                failover.append("dryRun", BsonBoolean.valueOf(failoverDryRun));
            }
            command.append("failover", failover);
        }

        return command;
    }
}
