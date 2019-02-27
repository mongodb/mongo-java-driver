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
import com.mongodb.MongoException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.WriteConcern;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.operation.CommandOperationHelper.CommandCreator;
import org.bson.BsonDocument;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL;
import static java.util.Arrays.asList;

/**
 * An operation that commits a transaction.
 *
 * @since 3.8
 */
@Deprecated
public class CommitTransactionOperation extends TransactionOperation {
    private final boolean alreadyCommitted;
    private BsonDocument recoveryToken;

    /**
     * Construct an instance.
     *
     * @param writeConcern the write concern
     */
    public CommitTransactionOperation(final WriteConcern writeConcern) {
        this(writeConcern, false);
    }

    /**
     * Construct an instance.
     *
     * @param writeConcern the write concern
     * @param alreadyCommitted if the transaction has already been committed.
     * @since 3.11
     */
    public CommitTransactionOperation(final WriteConcern writeConcern, final boolean alreadyCommitted) {
        super(writeConcern);
        this.alreadyCommitted = alreadyCommitted;
    }

    /**
     * Set the recovery token.
     *
     * @param recoveryToken the recovery token
     * @return the CommitTransactionOperation
     * @since 3.11
     */
    public CommitTransactionOperation recoveryToken(final BsonDocument recoveryToken) {
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
        super.executeAsync(binding, new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                 if (t instanceof MongoException) {
                     addErrorLabels((MongoException) t);
                 }
                 callback.onResult(result, t);
            }
        });
    }

    private void addErrorLabels(final MongoException e) {
        if (shouldAddUnknownTransactionCommitResultLabel(e)) {
            e.addLabel(UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL);
        }
    }

    private static final List<Integer> NON_RETRYABLE_WRITE_CONCERN_ERROR_CODES = asList(79, 100);

    static boolean shouldAddUnknownTransactionCommitResultLabel(final Throwable t) {
        if (!(t instanceof MongoException)) {
            return false;
        }

        MongoException e = (MongoException) t;

        if (e instanceof MongoSocketException || e instanceof MongoTimeoutException
                || e instanceof MongoNotPrimaryException || e instanceof MongoNodeIsRecoveringException) {
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
        final CommandCreator creator = super.getCommandCreator();
        if (alreadyCommitted) {
            return new CommandCreator() {
                @Override
                public BsonDocument create(final ServerDescription serverDescription, final ConnectionDescription connectionDescription) {
                    return getRetryCommandModifier().apply(creator.create(serverDescription, connectionDescription));
                }
            };
        } else if (recoveryToken != null) {
                return new CommandCreator() {
                    @Override
                    public BsonDocument create(final ServerDescription serverDescription,
                                               final ConnectionDescription connectionDescription) {
                        return creator.create(serverDescription, connectionDescription).append("recoveryToken", recoveryToken);
                    }
                };
        }
        return creator;
    }

    @Override
    protected Function<BsonDocument, BsonDocument> getRetryCommandModifier() {
        return new Function<BsonDocument, BsonDocument>() {
            @Override
            public BsonDocument apply(final BsonDocument command) {
                WriteConcern retryWriteConcern = getWriteConcern().withW("majority");
                if (retryWriteConcern.getWTimeout(TimeUnit.MILLISECONDS) == null) {
                    retryWriteConcern = retryWriteConcern.withWTimeout(10000, TimeUnit.MILLISECONDS);
                }
                command.put("writeConcern", retryWriteConcern.asDocument());
                if (recoveryToken != null) {
                    command.put("recoveryToken", recoveryToken);
                }
                return command;
            }
        };
    }
}
