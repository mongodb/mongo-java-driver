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
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeRetryableReadAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.SyncOperationHelper.executeRetryableRead;

/**
 * Returns runtime statistics for a stream processor. This is a retryable read.
 * Returns an error from the server if the processor is not in the {@code STARTED} state.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class GetStreamProcessorStatsOperation implements ReadOperationSimple<Document> {
    private static final String COMMAND_NAME = "getStreamProcessorStats";
    private static final String DATABASE = "admin";
    private static final DocumentCodec DECODER = new DocumentCodec();

    private final String processorName;
    private final boolean retryReads;
    @Nullable
    private final Boolean verbose;

    public GetStreamProcessorStatsOperation(final String processorName, final boolean retryReads,
                                            @Nullable final Boolean verbose) {
        this.processorName = notNull("processorName", processorName);
        this.retryReads = retryReads;
        this.verbose = verbose;
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
    public Document execute(final ReadBinding binding, final OperationContext operationContext) {
        return executeRetryableRead(binding, operationContext, DATABASE, getCommandCreator(), DECODER,
                (result, source, connection, ctx) -> result, retryReads);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final OperationContext operationContext,
                             final SingleResultCallback<Document> callback) {
        executeRetryableReadAsync(binding, operationContext, DATABASE, getCommandCreator(), DECODER,
                (result, source, connection, ctx) -> result, retryReads, callback);
    }

    private CommandCreator getCommandCreator() {
        return (operationContext, serverDescription, connectionDescription) -> {
            BsonDocument command = new BsonDocument(COMMAND_NAME, new BsonString(processorName));
            if (verbose != null) {
                command.append("options", new BsonDocument("verbose", BsonBoolean.valueOf(verbose)));
            }
            return command;
        };
    }
}
