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
import com.mongodb.WriteConcern;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.AsyncCallbackFunction;
import com.mongodb.internal.async.function.AsyncCallbackSupplier;
import com.mongodb.internal.bulk.DeleteRequest;
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsLessThanVersionFourDotFour;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsLessThanVersionFourDotTwo;
import static java.lang.String.format;

final class OperationHelper {
    public static final Logger LOGGER = Loggers.getLogger("operation");

    static void validateCollationAndWriteConcern(@Nullable final Collation collation, final WriteConcern writeConcern) {
        if (collation != null && !writeConcern.isAcknowledged()) {
            throw new MongoClientException("Specifying collation with an unacknowledged WriteConcern is not supported");
        }
    }

    private static void validateArrayFilters(final WriteConcern writeConcern) {
        if (!writeConcern.isAcknowledged()) {
            throw new MongoClientException("Specifying array filters with an unacknowledged WriteConcern is not supported");
        }
    }

    private static void validateWriteRequestHint(final ConnectionDescription connectionDescription, final WriteConcern writeConcern,
                                                 final WriteRequest request) {
        if (!writeConcern.isAcknowledged()) {
            if (request instanceof UpdateRequest && serverIsLessThanVersionFourDotTwo(connectionDescription)) {
                throw new IllegalArgumentException(format("Hint not supported by wire version: %s",
                        connectionDescription.getMaxWireVersion()));
            }
            if (request instanceof DeleteRequest && serverIsLessThanVersionFourDotFour(connectionDescription)) {
                throw new IllegalArgumentException(format("Hint not supported by wire version: %s",
                        connectionDescription.getMaxWireVersion()));
            }
        }
    }

    static void validateHintForFindAndModify(final ConnectionDescription connectionDescription, final WriteConcern writeConcern) {
        if (serverIsLessThanVersionFourDotTwo(connectionDescription)) {
            throw new IllegalArgumentException(format("Hint not supported by wire version: %s",
                    connectionDescription.getMaxWireVersion()));
        }
        if (!writeConcern.isAcknowledged() && serverIsLessThanVersionFourDotFour(connectionDescription)) {
            throw new IllegalArgumentException(format("Hint not supported by wire version: %s",
                    connectionDescription.getMaxWireVersion()));
        }
    }

    private static void validateWriteRequestCollations(final List<? extends WriteRequest> requests, final WriteConcern writeConcern) {
        Collation collation = null;
        for (WriteRequest request : requests) {
            if (request instanceof UpdateRequest) {
                collation = ((UpdateRequest) request).getCollation();
            } else if (request instanceof DeleteRequest) {
                collation = ((DeleteRequest) request).getCollation();
            }
            if (collation != null) {
                break;
            }
        }
        validateCollationAndWriteConcern(collation, writeConcern);
    }

    private static void validateUpdateRequestArrayFilters(final List<? extends WriteRequest> requests, final WriteConcern writeConcern) {
        for (WriteRequest request : requests) {
            List<BsonDocument> arrayFilters = null;
            if (request instanceof UpdateRequest) {
                arrayFilters = ((UpdateRequest) request).getArrayFilters();
            }
            if (arrayFilters != null) {
                validateArrayFilters(writeConcern);
                break;
            }
        }
    }

    private static void validateWriteRequestHints(final ConnectionDescription connectionDescription,
            final List<? extends WriteRequest> requests,
            final WriteConcern writeConcern) {
        for (WriteRequest request : requests) {
            Bson hint = null;
            String hintString = null;
            if (request instanceof UpdateRequest) {
                hint = ((UpdateRequest) request).getHint();
                hintString = ((UpdateRequest) request).getHintString();
            } else if (request instanceof DeleteRequest) {
                hint = ((DeleteRequest) request).getHint();
                hintString = ((DeleteRequest) request).getHintString();
            }
            if (hint != null || hintString != null) {
                validateWriteRequestHint(connectionDescription, writeConcern, request);
                break;
            }
        }
    }

    static void validateWriteRequests(final ConnectionDescription connectionDescription, final Boolean bypassDocumentValidation,
                                      final List<? extends WriteRequest> requests, final WriteConcern writeConcern) {
        checkBypassDocumentValidationIsSupported(bypassDocumentValidation, writeConcern);
        validateWriteRequestCollations(requests, writeConcern);
        validateUpdateRequestArrayFilters(requests, writeConcern);
        validateWriteRequestHints(connectionDescription, requests, writeConcern);
    }

    static <R> boolean validateWriteRequestsAndCompleteIfInvalid(final ConnectionDescription connectionDescription,
            final Boolean bypassDocumentValidation, final List<? extends WriteRequest> requests, final WriteConcern writeConcern,
            final SingleResultCallback<R> callback) {
        try {
            validateWriteRequests(connectionDescription, bypassDocumentValidation, requests, writeConcern);
            return false;
        } catch (Throwable validationT) {
            callback.onResult(null, validationT);
            return true;
        }
    }

    private static void checkBypassDocumentValidationIsSupported(@Nullable final Boolean bypassDocumentValidation,
            final WriteConcern writeConcern) {
        if (bypassDocumentValidation != null && !writeConcern.isAcknowledged()) {
            throw new MongoClientException("Specifying bypassDocumentValidation with an unacknowledged WriteConcern is not supported");
        }
    }

    static boolean isRetryableWrite(final boolean retryWrites, final WriteConcern writeConcern,
            final ConnectionDescription connectionDescription, final SessionContext sessionContext) {
        if (!retryWrites) {
            return false;
        } else if (!writeConcern.isAcknowledged()) {
            LOGGER.debug("retryWrites set to true but the writeConcern is unacknowledged.");
            return false;
        } else if (sessionContext.hasActiveTransaction()) {
            LOGGER.debug("retryWrites set to true but in an active transaction.");
            return false;
        } else {
            return canRetryWrite(connectionDescription, sessionContext);
        }
    }

    static boolean canRetryWrite(final ConnectionDescription connectionDescription, final SessionContext sessionContext) {
        if (connectionDescription.getLogicalSessionTimeoutMinutes() == null) {
            LOGGER.debug("retryWrites set to true but the server does not support sessions.");
            return false;
        } else if (connectionDescription.getServerType().equals(ServerType.STANDALONE)) {
            LOGGER.debug("retryWrites set to true but the server is a standalone server.");
            return false;
        }
        return true;
    }

    static boolean canRetryRead(final ServerDescription serverDescription, final OperationContext operationContext) {
        if (operationContext.getSessionContext().hasActiveTransaction()) {
            LOGGER.debug("retryReads set to true but in an active transaction.");
            return false;
        }
        return true;
    }

    static void addMaxTimeMSToNonTailableCursor(final BsonDocument commandDocument, final OperationContext operationContext) {
        addMaxTimeMSToNonTailableCursor(commandDocument, TimeoutMode.CURSOR_LIFETIME, operationContext);
    }

    static void addMaxTimeMSToNonTailableCursor(final BsonDocument command, final TimeoutMode timeoutMode,
            final OperationContext operationContext) {
        if (timeoutMode != TimeoutMode.ITERATION) {
            operationContext.getTimeoutContext().putMaxTimeMS(command);
        }
    }

    /**
     * This internal exception is used to
     * <ul>
     *     <li>on one hand allow propagating exceptions from {@link SyncOperationHelper#withSuppliedResource(Supplier, boolean, Function)} /
     *     {@link AsyncOperationHelper#withAsyncSuppliedResource(AsyncCallbackSupplier, boolean, SingleResultCallback, AsyncCallbackFunction)}
     *     and similar methods so that they can be properly retried, which is useful, e.g.,
     *     for {@link com.mongodb.MongoConnectionPoolClearedException};</li>
     *     <li>on the other hand to prevent them from propagation once the retry decision is made.</li>
     * </ul>
     *
     * @see SyncOperationHelper#withSuppliedResource(Supplier, boolean, Function)
     * @see AsyncOperationHelper#withAsyncSuppliedResource(AsyncCallbackSupplier, boolean, SingleResultCallback, AsyncCallbackFunction)
     */
    public static final class ResourceSupplierInternalException extends RuntimeException {
        private static final long serialVersionUID = 0;

        ResourceSupplierInternalException(final Throwable cause) {
            super(assertNotNull(cause));
        }

        @NonNull
        @Override
        public Throwable getCause() {
            return assertNotNull(super.getCause());
        }
    }

    private OperationHelper() {
    }
}
