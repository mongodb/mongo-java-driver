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
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoServerException;
import com.mongodb.MongoSocketException;
import com.mongodb.assertions.Assertions;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.async.function.RetryState;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.operation.OperationHelper.ResourceSupplierInternalException;
import com.mongodb.internal.operation.retry.AttachmentKeys;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import java.util.List;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static java.lang.String.format;
import static java.util.Arrays.asList;

@SuppressWarnings("overloads")
final class CommandOperationHelper {

    interface CommandCreator {
        BsonDocument create(
                OperationContext operationContext,
                ServerDescription serverDescription,
                ConnectionDescription connectionDescription);
    }


    static Throwable chooseRetryableReadException(
            @Nullable final Throwable previouslyChosenException, final Throwable mostRecentAttemptException) {
        assertFalse(mostRecentAttemptException instanceof ResourceSupplierInternalException);
        if (previouslyChosenException == null
                || mostRecentAttemptException instanceof MongoSocketException
                || mostRecentAttemptException instanceof MongoServerException) {
            return mostRecentAttemptException;
        } else {
            return previouslyChosenException;
        }
    }

    static Throwable chooseRetryableWriteException(
            @Nullable final Throwable previouslyChosenException, final Throwable mostRecentAttemptException) {
        if (previouslyChosenException == null) {
            if (mostRecentAttemptException instanceof ResourceSupplierInternalException) {
                return mostRecentAttemptException.getCause();
            }
            return mostRecentAttemptException;
        } else if (mostRecentAttemptException instanceof ResourceSupplierInternalException
                || (mostRecentAttemptException instanceof MongoException
                    && ((MongoException) mostRecentAttemptException).hasErrorLabel(NO_WRITES_PERFORMED_ERROR_LABEL))) {
            return previouslyChosenException;
        } else {
            return mostRecentAttemptException;
        }
    }

    /* Read Binding Helpers */

    static RetryState initialRetryState(final boolean retry) {
        return new RetryState(retry ? RetryState.RETRIES : 0);
    }

    private static final List<Integer> RETRYABLE_ERROR_CODES = asList(6, 7, 89, 91, 189, 262, 9001, 13436, 13435, 11602, 11600, 10107);
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

    static boolean shouldAttemptToRetryRead(final RetryState retryState, final Throwable attemptFailure) {
        assertFalse(attemptFailure instanceof ResourceSupplierInternalException);
        boolean decision = isRetryableException(attemptFailure)
                || (attemptFailure instanceof MongoSecurityException
                && attemptFailure.getCause() != null && isRetryableException(attemptFailure.getCause()));
        if (!decision) {
            logUnableToRetry(retryState.attachment(AttachmentKeys.commandDescriptionSupplier()).orElse(null), attemptFailure);
        }
        return decision;
    }

    static boolean shouldAttemptToRetryWrite(final RetryState retryState, final Throwable attemptFailure) {
        Throwable failure = attemptFailure instanceof ResourceSupplierInternalException ? attemptFailure.getCause() : attemptFailure;
        boolean decision = false;
        MongoException exceptionRetryableRegardlessOfCommand = null;
        if (failure instanceof MongoConnectionPoolClearedException
                || (failure instanceof MongoSecurityException && failure.getCause() != null && isRetryableException(failure.getCause()))) {
            decision = true;
            exceptionRetryableRegardlessOfCommand = (MongoException) failure;
        }
        if (retryState.attachment(AttachmentKeys.retryableCommandFlag()).orElse(false)) {
            if (exceptionRetryableRegardlessOfCommand != null) {
                /* We are going to retry even if `retryableCommand` is false,
                 * but we add the retryable label only if `retryableCommand` is true. */
                exceptionRetryableRegardlessOfCommand.addLabel(RETRYABLE_WRITE_ERROR_LABEL);
            } else if (decideRetryableAndAddRetryableWriteErrorLabel(failure, retryState.attachment(AttachmentKeys.maxWireVersion())
                    .orElse(null))) {
                decision = true;
            } else {
                logUnableToRetry(retryState.attachment(AttachmentKeys.commandDescriptionSupplier()).orElse(null), failure);
            }
        }
        return decision;
    }

    static boolean isRetryWritesEnabled(@Nullable final BsonDocument command) {
        return (command != null && (command.containsKey("txnNumber")
                || command.getFirstKey().equals("commitTransaction") || command.getFirstKey().equals("abortTransaction")));
    }

    static final String RETRYABLE_WRITE_ERROR_LABEL = "RetryableWriteError";
    private static final String NO_WRITES_PERFORMED_ERROR_LABEL = "NoWritesPerformed";

    private static boolean decideRetryableAndAddRetryableWriteErrorLabel(final Throwable t, @Nullable final Integer maxWireVersion) {
        if (!(t instanceof MongoException)) {
            return false;
        }
        MongoException exception = (MongoException) t;
        if (maxWireVersion != null) {
            addRetryableWriteErrorLabel(exception, maxWireVersion);
        }
        return exception.hasErrorLabel(RETRYABLE_WRITE_ERROR_LABEL);
    }

    static void addRetryableWriteErrorLabel(final MongoException exception, final int maxWireVersion) {
        if (maxWireVersion >= 9 && exception instanceof MongoSocketException) {
            exception.addLabel(RETRYABLE_WRITE_ERROR_LABEL);
        } else if (maxWireVersion < 9 && isRetryableException(exception)) {
            exception.addLabel(RETRYABLE_WRITE_ERROR_LABEL);
        }
    }

    static void logRetryExecute(final RetryState retryState, final OperationContext operationContext) {
        if (LOGGER.isDebugEnabled() && !retryState.isFirstAttempt()) {
            String commandDescription = retryState.attachment(AttachmentKeys.commandDescriptionSupplier()).map(Supplier::get).orElse(null);
            Throwable exception = retryState.exception().orElseThrow(Assertions::fail);
            int oneBasedAttempt = retryState.attempt() + 1;
            long operationId = operationContext.getId();
            LOGGER.debug(commandDescription == null
                    ? format("Retrying the operation with operation ID %s due to the error \"%s\". Attempt number: #%d",
                    operationId, exception, oneBasedAttempt)
                    : format("Retrying the operation '%s' with operation ID %s due to the error \"%s\". Attempt number: #%d",
                    commandDescription, operationId, exception, oneBasedAttempt));
        }
    }

    private static void logUnableToRetry(@Nullable final Supplier<String> commandDescriptionSupplier, final Throwable originalError) {
        if (LOGGER.isDebugEnabled()) {
            String commandDescription = commandDescriptionSupplier == null ? null : commandDescriptionSupplier.get();
            LOGGER.debug(commandDescription == null
                    ? format("Unable to retry an operation due to the error \"%s\"", originalError)
                    : format("Unable to retry the operation %s due to the error \"%s\"", commandDescription, originalError));
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
