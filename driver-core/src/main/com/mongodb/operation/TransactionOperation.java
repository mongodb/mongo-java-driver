/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.operation;

import com.mongodb.Function;
import com.mongodb.WriteConcern;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.operation.CommandOperationHelper.CommandCreator;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.codecs.BsonDocumentCodec;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.CommandOperationHelper.executeRetryableCommand;
import static com.mongodb.operation.CommandOperationHelper.writeConcernErrorTransformer;
import static com.mongodb.operation.OperationHelper.LOGGER;

/**
 * A base class for transaction-related operations
 *
 * @since 3.8
 */
@Deprecated
public abstract class TransactionOperation implements WriteOperation<Void>, AsyncWriteOperation<Void> {
    private final WriteConcern writeConcern;

    /**
     * Construct an instance.
     *
     * @param writeConcern the write concern
     */
    TransactionOperation(final WriteConcern writeConcern) {
        this.writeConcern = notNull("writeConcern", writeConcern);
    }

    /**
     * Gets the write concern.
     *
     * @return the write concern
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        isTrue("in transaction", binding.getSessionContext().hasActiveTransaction());
        return executeRetryableCommand(binding, "admin", null, new NoOpFieldNameValidator(),
                new BsonDocumentCodec(), getCommandCreator(), writeConcernErrorTransformer(), getRetryCommandModifier());
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        isTrue("in transaction", binding.getSessionContext().hasActiveTransaction());
        executeRetryableCommand(binding, "admin", null, new NoOpFieldNameValidator(),
                new BsonDocumentCodec(), getCommandCreator(), writeConcernErrorTransformer(), getRetryCommandModifier(),
                errorHandlingCallback(callback, LOGGER));
    }

    CommandCreator getCommandCreator() {
        return new CommandCreator() {
            @Override
            public BsonDocument create(final ServerDescription serverDescription, final ConnectionDescription connectionDescription) {
                BsonDocument command = new BsonDocument(getCommandName(), new BsonInt32(1));
                if (!writeConcern.isServerDefault()) {
                    command.put("writeConcern", writeConcern.asDocument());
                }
                return command;
            }
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
