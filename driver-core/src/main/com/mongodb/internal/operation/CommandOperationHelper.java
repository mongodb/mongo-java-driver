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
import com.mongodb.MongoClientException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSocketException;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import java.util.List;

import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static java.lang.String.format;
import static java.util.Arrays.asList;

@SuppressWarnings("overloads")
final class CommandOperationHelper {

    static Function<BsonDocument, BsonDocument> noOpRetryCommandModifier() {
        return command -> command;
    }

    interface CommandCreator {
        BsonDocument create(ServerDescription serverDescription, ConnectionDescription connectionDescription);
    }

    /* Retryable helpers */
    private static final List<Integer> RETRYABLE_ERROR_CODES = asList(6, 7, 89, 91, 189, 262, 9001, 13436, 13435, 11602, 11600, 10107);

    static boolean isRetryableException(final Throwable t) {
        if (!(t instanceof MongoException)) {
            return false;
        }

        if (t instanceof MongoSocketException || t instanceof MongoNotPrimaryException || t instanceof MongoNodeIsRecoveringException) {
            return true;
        }
        return RETRYABLE_ERROR_CODES.contains(((MongoException) t).getCode());
    }

    /* Misc operation helpers */
    static void rethrowIfNotNamespaceError(final MongoCommandException e) {
        rethrowIfNotNamespaceError(e, null);
    }

    static <T> T rethrowIfNotNamespaceError(final MongoCommandException e, final T defaultValue) {
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

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    static boolean shouldAttemptToRetryRead(final boolean retryReadsEnabled, final Throwable t) {
        return retryReadsEnabled && isRetryableException(t);
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    static boolean shouldAttemptToRetryWrite(@Nullable final BsonDocument command, final Throwable t,
                                             final int maxWireVersion) {
        return shouldAttemptToRetryWrite(isRetryWritesEnabled(command), t, maxWireVersion);
    }

    static boolean isRetryWritesEnabled(@Nullable final BsonDocument command) {
        return (command != null && (command.containsKey("txnNumber")
                || command.getFirstKey().equals("commitTransaction") || command.getFirstKey().equals("abortTransaction")));
    }

    static final String RETRYABLE_WRITE_ERROR_LABEL = "RetryableWriteError";

    static boolean shouldAttemptToRetryWrite(final boolean retryWritesEnabled, final Throwable t, final int maxWireVersion) {
        if (!retryWritesEnabled) {
            return false;
        } else if (!(t instanceof MongoException)) {
            return false;
        }

        MongoException exception = (MongoException) t;
        addRetryableWriteErrorLabel(exception, maxWireVersion);
        return exception.hasErrorLabel(RETRYABLE_WRITE_ERROR_LABEL);
    }

    static void addRetryableWriteErrorLabel(final MongoException exception, final int maxWireVersion) {
        if (maxWireVersion >= 9 && exception instanceof MongoSocketException) {
            exception.addLabel(RETRYABLE_WRITE_ERROR_LABEL);
        } else if (maxWireVersion < 9 && isRetryableException(exception)) {
            exception.addLabel(RETRYABLE_WRITE_ERROR_LABEL);
        }
    }

    static void logRetryExecute(final String operation, final Throwable originalError) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Retrying operation %s due to an error \"%s\"", operation, originalError));
        }
    }

    static void logUnableToRetry(final String operation, final Throwable originalError) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Unable to retry operation %s due to error \"%s\"", operation, originalError));
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
