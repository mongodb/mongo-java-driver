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
import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * An operation that commits a transaction.
 *
 * @since 3.8
 */
public class CommitTransactionOperation extends TransactionOperation {
    private final boolean alreadyCommitted;
    private BsonDocument recoveryToken;
    private Long maxCommitTimeMS;

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

    /**
     * Sets the maximum execution time on the server for the commitTransaction operation.
     *
     * @param maxCommitTime  the max commit time, which must be either null or greater than zero, in the given time unit
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @since 3.11
     * @mongodb.server.release 4.2
     */
    public CommitTransactionOperation maxCommitTime(@Nullable final Long maxCommitTime, final TimeUnit timeUnit) {
        if (maxCommitTime == null) {
            this.maxCommitTimeMS = null;
        } else {
            notNull("timeUnit", timeUnit);
            isTrueArgument("maxCommitTime > 0", maxCommitTime > 0);
            this.maxCommitTimeMS = MILLISECONDS.convert(maxCommitTime, timeUnit);
        }
        return this;
    }

    /**
     * Gets the maximum amount of time to allow a single commitTransaction command to execute.  The default is 0, which places no limit on
     * the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     * @since 3.11
     * @mongodb.server.release 4.2
     */
    @Nullable
    public Long getMaxCommitTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        if (maxCommitTimeMS == null) {
            return null;
        }
        return timeUnit.convert(maxCommitTimeMS, MILLISECONDS);
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

    private static boolean shouldAddUnknownTransactionCommitResultLabel(final Throwable t) {
        if (!(t instanceof MongoException)) {
            return false;
        }

        MongoException e = (MongoException) t;

        if (e instanceof MongoSocketException || e instanceof MongoTimeoutException
                || e instanceof MongoNotPrimaryException || e instanceof MongoNodeIsRecoveringException
                || e instanceof MongoExecutionTimeoutException) {
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
        final CommandCreator creator = new CommandCreator() {
            @Override
            public BsonDocument create(final ServerDescription serverDescription, final ConnectionDescription connectionDescription) {
                BsonDocument command = CommitTransactionOperation.super.getCommandCreator().create(serverDescription,
                        connectionDescription);
                if (maxCommitTimeMS != null) {
                    command.append("maxTimeMS",
                            maxCommitTimeMS > Integer.MAX_VALUE
                            ? new BsonInt64(maxCommitTimeMS) : new BsonInt32(maxCommitTimeMS.intValue()));
                }
                return command;
            }
        };
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
