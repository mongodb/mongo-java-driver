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

import com.mongodb.Function;
import com.mongodb.WriteConcern;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.codecs.BsonDocumentCodec;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeRetryableWriteAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.writeConcernErrorTransformerAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.SyncOperationHelper.executeRetryableWrite;
import static com.mongodb.internal.operation.SyncOperationHelper.writeConcernErrorTransformer;

/**
 * A base class for transaction-related operations
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public abstract class TransactionOperation implements WriteOperation<Void>, AsyncWriteOperation<Void> {
    private final TimeoutSettings timeoutSettings;
    private final WriteConcern writeConcern;

    TransactionOperation(final TimeoutSettings timeoutSettings, final WriteConcern writeConcern) {
        this.timeoutSettings = timeoutSettings;
        this.writeConcern = notNull("writeConcern", writeConcern);
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public TimeoutSettings getTimeoutSettings() {
        return timeoutSettings;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        isTrue("in transaction", binding.getOperationContext().getSessionContext().hasActiveTransaction());
        return executeRetryableWrite(binding, "admin", null, new NoOpFieldNameValidator(),
                                     new BsonDocumentCodec(), getCommandCreator(), writeConcernErrorTransformer(), getRetryCommandModifier());
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        isTrue("in transaction", binding.getOperationContext().getSessionContext().hasActiveTransaction());
        executeRetryableWriteAsync(binding, "admin", null, new NoOpFieldNameValidator(),
                                   new BsonDocumentCodec(), getCommandCreator(), writeConcernErrorTransformerAsync(), getRetryCommandModifier(),
                                   errorHandlingCallback(callback, LOGGER));
    }

    CommandCreator getCommandCreator() {
        return (operationContext, serverDescription, connectionDescription) -> {
            BsonDocument command = new BsonDocument(getCommandName(), new BsonInt32(1));
            if (!writeConcern.isServerDefault()) {
                command.put("writeConcern", writeConcern.asDocument());
            }
            return command;
        };
    }

    /**
     * Gets the command name.
     *
     * @return the command name
     */
    protected abstract String getCommandName();

    protected abstract Function<BsonDocument, BsonDocument> getRetryCommandModifier();
}
