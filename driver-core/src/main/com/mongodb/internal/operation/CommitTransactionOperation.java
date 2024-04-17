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
import com.mongodb.MongoException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.WriteConcern;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import java.util.List;

import static com.mongodb.MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.CommandOperationHelper.RETRYABLE_WRITE_ERROR_LABEL;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * An operation that commits a transaction.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class CommitTransactionOperation extends TransactionOperation {
    private final boolean alreadyCommitted;
    private BsonDocument recoveryToken;

    public CommitTransactionOperation(final WriteConcern writeConcern) {
        this(writeConcern, false);
    }

    public CommitTransactionOperation(final WriteConcern writeConcern, final boolean alreadyCommitted) {
        super(writeConcern);
        this.alreadyCommitted = alreadyCommitted;
    }

    public CommitTransactionOperation recoveryToken(@Nullable final BsonDocument recoveryToken) {
        this.recoveryToken = recoveryToken;
        return this;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        try {
            return super.execute(binding);
        } catch (MongoException e) {
            addErrorLabels(e);
            throw e;
        }
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        super.executeAsync(binding, (result, t) -> {
             if (t instanceof MongoException) {
                 addErrorLabels((MongoException) t);
             }
             callback.onResult(result, t);
        });
    }

    private void addErrorLabels(final MongoException e) {
        if (shouldAddUnknownTransactionCommitResultLabel(e)) {
            e.addLabel(UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL);
        }
    }

    private static final List<Integer> NON_RETRYABLE_WRITE_CONCERN_ERROR_CODES = asList(79, 100);

    private static boolean shouldAddUnknownTransactionCommitResultLabel(final MongoException e) {

        if (e instanceof MongoSocketException || e instanceof MongoTimeoutException
                || e instanceof MongoNotPrimaryException || e instanceof MongoNodeIsRecoveringException
                || e instanceof MongoExecutionTimeoutException) {
            return true;
        }

        if (e.hasErrorLabel(RETRYABLE_WRITE_ERROR_LABEL)) {
            return true;
        }

        if (e instanceof MongoWriteConcernException) {
            return !NON_RETRYABLE_WRITE_CONCERN_ERROR_CODES.contains(e.getCode());
        }

        return false;
    }


    @Override
    protected String getCommandName() {
        return "commitTransaction";
    }

    @Override
    CommandCreator getCommandCreator() {
        CommandCreator creator = (operationContext, serverDescription, connectionDescription) -> {
            BsonDocument command = CommitTransactionOperation.super.getCommandCreator()
                    .create(operationContext, serverDescription, connectionDescription);
            operationContext.getTimeoutContext().setMaxTimeOverrideToMaxCommitTime();
            return command;
        };
        if (alreadyCommitted) {
            return (operationContext, serverDescription, connectionDescription) ->
                    getRetryCommandModifier().apply(creator.create(operationContext, serverDescription, connectionDescription));
        } else if (recoveryToken != null) {
                return (operationContext, serverDescription, connectionDescription) ->
                        creator.create(operationContext, serverDescription, connectionDescription)
                                .append("recoveryToken", recoveryToken);
        }
        return creator;
    }

    @Override
    protected Function<BsonDocument, BsonDocument> getRetryCommandModifier() {
        return command -> {
            WriteConcern retryWriteConcern = getWriteConcern().withW("majority");
            if (retryWriteConcern.getWTimeout(MILLISECONDS) == null) {
                retryWriteConcern = retryWriteConcern.withWTimeout(10000, MILLISECONDS);
            }
            command.put("writeConcern", retryWriteConcern.asDocument());
            if (recoveryToken != null) {
                command.put("recoveryToken", recoveryToken);
            }
            return command;
        };
    }
}
