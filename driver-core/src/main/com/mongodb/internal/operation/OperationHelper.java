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
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.bulk.DeleteRequest;
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.connection.QueryResult;
import com.mongodb.internal.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.Optional;

import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotFour;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotTwo;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsLessThanVersionFourDotTwo;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsLessThanVersionThreeDotFour;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsLessThanVersionThreeDotSix;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsLessThanVersionThreeDotTwo;
import static java.lang.String.format;

final class OperationHelper {
    public static final Logger LOGGER = Loggers.getLogger("operation");

    static void validateReadConcern(final ConnectionDescription description, final ReadConcern readConcern) {
        if (!serverIsAtLeastVersionThreeDotTwo(description) && !readConcern.isServerDefault()) {
            throw new IllegalArgumentException(format("ReadConcern not supported by wire version: %s",
                    description.getMaxWireVersion()));
        }
    }

    static void validateCollation(final ConnectionDescription connectionDescription, final Collation collation) {
        if (collation != null && !serverIsAtLeastVersionThreeDotFour(connectionDescription)) {
            throw new IllegalArgumentException(format("Collation not supported by wire version: %s",
                    connectionDescription.getMaxWireVersion()));
        }
    }

    static void validateCollationAndWriteConcern(final ConnectionDescription connectionDescription, final Collation collation,
                                                 final WriteConcern writeConcern) {
        if (collation != null && !serverIsAtLeastVersionThreeDotFour(connectionDescription)) {
            throw new IllegalArgumentException(format("Collation not supported by wire version: %s",
                    connectionDescription.getMaxWireVersion()));
        } else if (collation != null && !writeConcern.isAcknowledged()) {
            throw new MongoClientException("Specifying collation with an unacknowledged WriteConcern is not supported");
        }
    }

    private static void validateArrayFilters(final ConnectionDescription connectionDescription, final WriteConcern writeConcern) {
        if (serverIsLessThanVersionThreeDotSix(connectionDescription)) {
            throw new IllegalArgumentException(format("Array filters not supported by wire version: %s",
                    connectionDescription.getMaxWireVersion()));
        } else if (!writeConcern.isAcknowledged()) {
            throw new MongoClientException("Specifying array filters with an unacknowledged WriteConcern is not supported");
        }
    }

    private static void validateWriteRequestHint(final ConnectionDescription connectionDescription, final WriteConcern writeConcern,
                                                 final WriteRequest request) {
        if (serverIsLessThanVersionThreeDotFour(connectionDescription)) {
            throw new IllegalArgumentException(format("Hint not supported by wire version: %s",
                    connectionDescription.getMaxWireVersion()));
        } else if ((request instanceof DeleteRequest || request instanceof UpdateRequest) && !writeConcern.isAcknowledged()) {
            throw new MongoClientException("Specifying hints with an unacknowledged WriteConcern is not supported");
        }
    }

    static void validateHint(final ConnectionDescription connectionDescription, final WriteConcern writeConcern) {
        if (serverIsLessThanVersionFourDotTwo(connectionDescription)) {
            throw new IllegalArgumentException(format("Hint not supported by wire version: %s",
                    connectionDescription.getMaxWireVersion()));
        } else if (!writeConcern.isAcknowledged()) {
            throw new MongoClientException("Specifying hints with an unacknowledged WriteConcern is not supported");
        }
    }

    static void validateWriteRequestCollations(final ConnectionDescription connectionDescription,
                                               final List<? extends WriteRequest> requests, final WriteConcern writeConcern) {
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
        validateCollationAndWriteConcern(connectionDescription, collation, writeConcern);
    }

    static void validateUpdateRequestArrayFilters(final ConnectionDescription connectionDescription,
                                                  final List<? extends WriteRequest> requests, final WriteConcern writeConcern) {
        for (WriteRequest request : requests) {
            List<BsonDocument> arrayFilters = null;
            if (request instanceof UpdateRequest) {
                arrayFilters = ((UpdateRequest) request).getArrayFilters();
            }
            if (arrayFilters != null) {
                validateArrayFilters(connectionDescription, writeConcern);
                break;
            }
        }
    }

    static void validateWriteRequestHints(final ConnectionDescription connectionDescription,
                                          final List<? extends WriteRequest> requests, final WriteConcern writeConcern) {
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
        checkBypassDocumentValidationIsSupported(connectionDescription, bypassDocumentValidation, writeConcern);
        validateWriteRequestCollations(connectionDescription, requests, writeConcern);
        validateUpdateRequestArrayFilters(connectionDescription, requests, writeConcern);
        validateWriteRequestHints(connectionDescription, requests, writeConcern);
    }

    static void validateFindOptions(final ConnectionDescription description, final ReadConcern readConcern,
                                    final Collation collation, final Boolean allowDiskUse) {
        validateReadConcernAndCollation(description, readConcern, collation);
        validateAllowDiskUse(description, allowDiskUse).ifPresent(throwable -> {
            throw new IllegalArgumentException(throwable.getMessage());
        });
    }

    static void validateReadConcernAndCollation(final ConnectionDescription description, final ReadConcern readConcern,
                                                final Collation collation) {
        validateReadConcern(description, readConcern);
        validateCollation(description, collation);
    }

    static void checkBypassDocumentValidationIsSupported(final ConnectionDescription connectionDescription,
                                                         final Boolean bypassDocumentValidation, final WriteConcern writeConcern) {
        if (bypassDocumentValidation != null && serverIsAtLeastVersionThreeDotTwo(connectionDescription)
                && !writeConcern.isAcknowledged()) {
            throw new MongoClientException("Specifying bypassDocumentValidation with an unacknowledged WriteConcern is not supported");
        }
    }

    static boolean isRetryableWrite(final boolean retryWrites, final WriteConcern writeConcern,
                                    final ServerDescription serverDescription, final ConnectionDescription connectionDescription,
                                    final SessionContext sessionContext) {
        if (!retryWrites) {
            return false;
        } else if (!writeConcern.isAcknowledged()) {
            LOGGER.debug("retryWrites set to true but the writeConcern is unacknowledged.");
            return false;
        } else if (sessionContext.hasActiveTransaction()) {
            LOGGER.debug("retryWrites set to true but in an active transaction.");
            return false;
        } else {
            return canRetryWrite(serverDescription, connectionDescription, sessionContext);
        }
    }

    static boolean canRetryWrite(final ServerDescription serverDescription, final ConnectionDescription connectionDescription,
                                 final SessionContext sessionContext) {
        if (serverIsLessThanVersionThreeDotSix(connectionDescription)) {
            LOGGER.debug("retryWrites set to true but the server does not support retryable writes.");
            return false;
        } else if (serverDescription.getLogicalSessionTimeoutMinutes() == null && serverDescription.getType() != ServerType.LOAD_BALANCER) {
            LOGGER.debug("retryWrites set to true but the server does not have 3.6 feature compatibility enabled.");
            return false;
        } else if (connectionDescription.getServerType().equals(ServerType.STANDALONE)) {
            LOGGER.debug("retryWrites set to true but the server is a standalone server.");
            return false;
        } else if (!sessionContext.hasSession()) {
            LOGGER.debug("retryWrites set to true but there is no implicit session, likely because the MongoClient was created with "
                    + "multiple MongoCredential instances and sessions can only be used with a single MongoCredential");
            return false;
        }
        return true;
    }

    static boolean isRetryableRead(final boolean retryReads, final ServerDescription serverDescription,
                                   final ConnectionDescription connectionDescription, final SessionContext sessionContext) {
        if (!retryReads) {
            return false;
        } else if (sessionContext.hasActiveTransaction()) {
            LOGGER.debug("retryReads set to true but in an active transaction.");
            return false;
        } else {
            return canRetryRead(serverDescription, connectionDescription, sessionContext);
        }
    }

    static boolean canRetryRead(final ServerDescription serverDescription, final ConnectionDescription connectionDescription,
                                final SessionContext sessionContext) {
        if (serverIsLessThanVersionThreeDotSix(connectionDescription)) {
            LOGGER.debug("retryReads set to true but the server does not support retryable reads.");
            return false;
        } else if (serverDescription.getLogicalSessionTimeoutMinutes() == null && serverDescription.getType() != ServerType.LOAD_BALANCER) {
            LOGGER.debug("retryReads set to true but the server does not have 3.6 feature compatibility enabled.");
            return false;
        } else if (serverDescription.getType() != ServerType.STANDALONE && !sessionContext.hasSession()) {
            LOGGER.debug("retryReads set to true but there is no implicit session, likely because the MongoClient was created with "
                    + "multiple MongoCredential instances and sessions can only be used with a single MongoCredential");
            return false;
        }
        return true;
    }

    static Optional<Throwable> validateAllowDiskUse(final ConnectionDescription description, final Boolean allowDiskUse) {
        Optional<Throwable> throwable = Optional.empty();
        if (allowDiskUse != null && serverIsLessThanVersionThreeDotTwo(description)) {
            throwable = Optional.of(new IllegalArgumentException(format("allowDiskUse not supported by wire version: %s",
                    description.getMaxWireVersion())));
        }
        return throwable;
    }

    private OperationHelper() {
    }

    static <T> QueryResult<T> cursorDocumentToQueryResult(final BsonDocument cursorDocument, final ServerAddress serverAddress) {
        return cursorDocumentToQueryResult(cursorDocument, serverAddress, "firstBatch");
    }

    static <T> QueryResult<T> getMoreCursorDocumentToQueryResult(final BsonDocument cursorDocument, final ServerAddress serverAddress) {
        return cursorDocumentToQueryResult(cursorDocument, serverAddress, "nextBatch");
    }

    private static <T> QueryResult<T> cursorDocumentToQueryResult(final BsonDocument cursorDocument, final ServerAddress serverAddress,
                                                                  final String fieldNameContainingBatch) {
        long cursorId = ((BsonInt64) cursorDocument.get("id")).getValue();
        MongoNamespace queryResultNamespace = new MongoNamespace(cursorDocument.getString("ns").getValue());
        return new QueryResult<>(queryResultNamespace, BsonDocumentWrapperHelper.toList(cursorDocument, fieldNameContainingBatch),
                cursorId, serverAddress);
    }
}
