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

import com.mongodb.MongoClientException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.MongoException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSocketException;
import com.mongodb.WriteConcern;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.async.function.RetryControl;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.operation.SpecRetryPolicy.ExplicitMaxRetries;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import java.util.List;
import java.util.Optional;

import static com.mongodb.internal.operation.SpecRetryPolicy.ExplicitMaxRetries.RETRIES_LIMITED_BY_INDIVIDUAL_POLICIES;
import static com.mongodb.internal.operation.SpecRetryPolicy.ExplicitMaxRetries.NO_RETRIES_LIMIT;
import static java.util.Arrays.asList;

@SuppressWarnings("overloads")
public final class CommandOperationHelper {
    static WriteConcern validateAndGetEffectiveWriteConcern(final WriteConcern writeConcernSetting, final SessionContext sessionContext)
            throws MongoClientException {
        boolean activeTransaction = sessionContext.hasActiveTransaction();
        WriteConcern effectiveWriteConcern = activeTransaction
                ? WriteConcern.ACKNOWLEDGED
                : writeConcernSetting;
        if (sessionContext.hasSession() && !sessionContext.isImplicitSession() && !activeTransaction && !effectiveWriteConcern.isAcknowledged()) {
            throw new MongoClientException("Unacknowledged writes are not supported when using an explicit session");
        }
        return effectiveWriteConcern;
    }

    static Optional<WriteConcern> commandWriteConcern(final WriteConcern effectiveWriteConcern, final SessionContext sessionContext) {
        return effectiveWriteConcern.isServerDefault() || sessionContext.hasActiveTransaction()
                ? Optional.empty()
                : Optional.of(effectiveWriteConcern);
    }

    interface CommandCreator {
        BsonDocument create(
                OperationContext operationContext,
                ServerDescription serverDescription,
                ConnectionDescription connectionDescription);
    }

    /* Read Binding Helpers */

    static RetryControl<SpecRetryPolicy> createSpecRetryControl(
            final SpecRetryPolicy.IndividualPolicies policies,
            final OperationContext operationContext) {
        ExplicitMaxRetries explicitMaxRetries = operationContext.getTimeoutContext().hasTimeoutMS()
                ? NO_RETRIES_LIMIT
                : RETRIES_LIMITED_BY_INDIVIDUAL_POLICIES;
        return new RetryControl<>(new SpecRetryPolicy(
                policies,
                explicitMaxRetries,
                operationContext.getServerDeprioritization()));
    }

    private static final List<Integer> RETRYABLE_ERROR_CODES = asList(6, 7, 89, 91, 134, 189, 262, 9001, 13436, 13435, 11602, 11600, 10107);
    static boolean isRetryableException(final Throwable t) {
        if (!(t instanceof MongoException)) {
            return false;
        }

        if (t instanceof MongoSocketException || t instanceof MongoNotPrimaryException || t instanceof MongoNodeIsRecoveringException
                || t instanceof MongoConnectionPoolClearedException) {
            return true;
        }
        return RETRYABLE_ERROR_CODES.contains(((MongoException) t).getCode());
    }

    /* Misc operation helpers */

    static void rethrowIfNotNamespaceError(final MongoCommandException e) {
        rethrowIfNotNamespaceError(e, null);
    }

    @Nullable
    static <T> T rethrowIfNotNamespaceError(final MongoCommandException e, @Nullable final T defaultValue) {
        if (!isNamespaceError(e)) {
            throw e;
        }
        return defaultValue;
    }

    static boolean isNamespaceError(final Throwable t) {
        if (t instanceof MongoCommandException) {
            MongoCommandException e = (MongoCommandException) t;
            return (e.getErrorMessage().contains("ns not found") || e.getErrorCode() == 26);
        } else {
            return false;
        }
    }

    static boolean isWriteRetryRequirementsMet(final BsonDocument command) {
        // Given the requirement
        // https://github.com/mongodb/specifications/blame/7039e69945d463a14b1b727d16db063e21f48f53/source/transactions/transactions.md#L584-L586:
        //   When executing the `commitTransaction` and `abortTransaction` commands within a transaction
        //   drivers MUST use the same `txnNumber` used for all preceding commands in the transaction.
        // the additional checks if the `command` is either `commitTransaction`/`abortTransaction`, may seem unnecessary.
        // However, since the `txnNumber` key is added to commands within transactions by `CommandMessage`,
        // the key is not present when the logic of automatic retries inspects a `commitTransaction`/`abortTransaction` command for it.
        return (command.containsKey("txnNumber")
                || command.getFirstKey().equals("commitTransaction") || command.getFirstKey().equals("abortTransaction"));
    }

    public static final String RETRYABLE_WRITE_ERROR_LABEL = "RetryableWriteError";
    public static final String NO_WRITES_PERFORMED_ERROR_LABEL = "NoWritesPerformed";

    static void addRetryableWriteErrorLabelIfNeeded(final MongoException exception, final int maxWireVersion) {
        if (maxWireVersion >= 9 && exception instanceof MongoSocketException) {
            exception.addLabel(RETRYABLE_WRITE_ERROR_LABEL);
        } else if (maxWireVersion < 9 && isRetryableException(exception)) {
            exception.addLabel(RETRYABLE_WRITE_ERROR_LABEL);
        }
    }

    static MongoException transformWriteException(final MongoException exception) {
        if (exception.getCode() == 20 && exception.getMessage().contains("Transaction numbers")) {
            MongoException clientException = new MongoClientException("This MongoDB deployment does not support retryable writes. "
                    + "Please add retryWrites=false to your connection string.", exception);
            for (final String errorLabel : exception.getErrorLabels()) {
                clientException.addLabel(errorLabel);
            }
            return clientException;
        }
        return exception;
    }

    private CommandOperationHelper() {
    }
}
