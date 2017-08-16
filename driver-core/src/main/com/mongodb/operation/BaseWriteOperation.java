/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.operation;

import com.mongodb.DuplicateKeyException;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernException;
import com.mongodb.WriteConcernResult;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.WriteRequest;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.bulk.WriteRequest.Type.DELETE;
import static com.mongodb.bulk.WriteRequest.Type.INSERT;
import static com.mongodb.bulk.WriteRequest.Type.REPLACE;
import static com.mongodb.bulk.WriteRequest.Type.UPDATE;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnection;
import static com.mongodb.operation.OperationHelper.CallableWithConnection;
import static com.mongodb.operation.OperationHelper.LOGGER;
import static com.mongodb.operation.OperationHelper.checkBypassDocumentValidationIsSupported;
import static com.mongodb.operation.OperationHelper.releasingCallback;
import static com.mongodb.operation.OperationHelper.withConnection;


/**
 * Abstract base class for write operations.
 *
 * @since 3.0
 */
public abstract class BaseWriteOperation implements AsyncWriteOperation<WriteConcernResult>, WriteOperation<WriteConcernResult> {

    private final WriteConcern writeConcern;
    private final MongoNamespace namespace;
    private final boolean ordered;
    private Boolean bypassDocumentValidation;

    /**
     * Construct an instance
     *
     * @param namespace    the database and collection namespace for the operation.
     * @param ordered      whether the writes are ordered.
     * @param writeConcern the write concern for the operation.
     */
    public BaseWriteOperation(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern) {
        this.ordered = ordered;
        this.namespace = notNull("namespace", namespace);
        this.writeConcern = notNull("writeConcern", writeConcern);
    }

    /**
     * Gets the namespace of the collection to write to.
     *
     * @return the namespace
     */
    public MongoNamespace getNamespace() {
        return namespace;
    }

    /**
     * Gets the write concern to apply
     *
     * @return the write concern
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * Gets whether the writes are ordered.  If true, no more writes will be executed after the first failure.
     *
     * @return whether the writes are ordered
     */
    public boolean isOrdered() {
        return ordered;
    }

    /**
     * Gets the the bypass document level validation flag
     *
     * @return the bypass document level validation flag
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    /**
     * Sets the bypass document level validation flag.
     *
     * @param bypassDocumentValidation If true, allows the write to opt-out of document level validation.
     * @return this
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public BaseWriteOperation bypassDocumentValidation(final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    @Override
    public WriteConcernResult execute(final WriteBinding binding) {
        return withConnection(binding, new CallableWithConnection<WriteConcernResult>() {
            @Override
            public WriteConcernResult call(final Connection connection) {
                try {
                    checkBypassDocumentValidationIsSupported(connection, bypassDocumentValidation, writeConcern);
                    if (writeConcern.isAcknowledged()) {
                        return translateBulkWriteResult(executeCommandProtocol(connection));
                    } else {
                        return executeProtocol(connection);
                    }
                } catch (MongoBulkWriteException e) {
                    throw convertBulkWriteException(e);
                }
            }
        });
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<WriteConcernResult> callback) {
        withConnection(binding, new AsyncCallableWithConnection() {
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                final SingleResultCallback<WriteConcernResult> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    checkBypassDocumentValidationIsSupported(connection, bypassDocumentValidation, writeConcern,
                            new AsyncCallableWithConnection() {
                                @Override
                                public void call(final AsyncConnection connection, final Throwable t1) {
                                    if (t1 != null) {
                                        releasingCallback(errHandlingCallback, connection).onResult(null, t1);
                                    } else {
                                        final SingleResultCallback<WriteConcernResult> wrappedCallback =
                                                releasingCallback(errHandlingCallback, connection);
                                        if (writeConcern.isAcknowledged()) {
                                            executeCommandProtocolAsync(connection, new SingleResultCallback<BulkWriteResult>() {
                                                @Override
                                                public void onResult(final BulkWriteResult result, final Throwable t) {
                                                    if (t != null) {
                                                        wrappedCallback.onResult(null, translateException(t));
                                                    } else {
                                                        wrappedCallback.onResult(translateBulkWriteResult(result), null);
                                                    }
                                                }
                                            });
                                        } else {
                                            executeProtocolAsync(connection, new SingleResultCallback<WriteConcernResult>() {
                                                @Override
                                                public void onResult(final WriteConcernResult result, final Throwable t) {
                                                    if (t != null) {
                                                        wrappedCallback.onResult(null, translateException(t));
                                                    } else {
                                                        wrappedCallback.onResult(result, null);
                                                    }
                                                }
                                            });
                                        }
                                    }
                                }
                            });
                }
            }
        });
    }

    /**
     * Executes the write protocol
     *
     * @param connection the connection
     * @return the write protocol
     */
    protected abstract WriteConcernResult executeProtocol(Connection connection);

    /**
     * Asynchronously executes the write protocol
     *  @param connection the connection
     * @param callback   the callback to be passed the WriteConcernResult
     */
    protected abstract void executeProtocolAsync(AsyncConnection connection, SingleResultCallback<WriteConcernResult> callback);

    /**
     * Executes the write command protocol.
     *
     * @param connection the connection
     * @return the result
     */
    protected abstract BulkWriteResult executeCommandProtocol(Connection connection);

    /**
     * Asynchronously executes the write command protocol.
     *  @param connection the connection
     * @param callback   the callback to be passed the BulkWriteResult
     */
    protected abstract void executeCommandProtocolAsync(AsyncConnection connection, SingleResultCallback<BulkWriteResult> callback);

    private MongoException translateException(final Throwable t) {
        MongoException checkedError = MongoException.fromThrowable(t);
        if (t instanceof MongoBulkWriteException) {
            checkedError = convertBulkWriteException((MongoBulkWriteException) t);
        }
        return checkedError;
    }

    @SuppressWarnings("deprecation")
    private MongoException convertBulkWriteException(final MongoBulkWriteException e) {
        BulkWriteError lastError = getLastError(e);
        if (lastError != null) {
            if (ErrorCategory.fromErrorCode(lastError.getCode()) == ErrorCategory.DUPLICATE_KEY) {
                return new DuplicateKeyException(manufactureGetLastErrorResponse(e), e.getServerAddress(),
                                                      translateBulkWriteResult(e.getWriteResult()));
            } else {
                return new WriteConcernException(manufactureGetLastErrorResponse(e), e.getServerAddress(),
                                                 translateBulkWriteResult(e.getWriteResult()));
            }
        } else {
            return new WriteConcernException(manufactureGetLastErrorResponse(e), e.getServerAddress(),
                                             translateBulkWriteResult(e.getWriteResult()));
        }

    }

    private BsonDocument manufactureGetLastErrorResponse(final MongoBulkWriteException e) {
        BsonDocument response = new BsonDocument();
        addBulkWriteResultToResponse(e.getWriteResult(), response);
        if (e.getWriteConcernError() != null) {
            response.putAll(e.getWriteConcernError().getDetails());
        }
        if (getLastError(e) != null) {
            response.put("err", new BsonString(getLastError(e).getMessage()));
            response.put("code", new BsonInt32(getLastError(e).getCode()));
            response.putAll(getLastError(e).getDetails());

        } else if (e.getWriteConcernError() != null) {
            response.put("err", new BsonString(e.getWriteConcernError().getMessage()));
            response.put("code", new BsonInt32(e.getWriteConcernError().getCode()));
        }
        return response;
    }

    private void addBulkWriteResultToResponse(final BulkWriteResult bulkWriteResult, final BsonDocument response) {
        response.put("ok", new BsonInt32(1));
        if (getType() == INSERT) {
            response.put("n", new BsonInt32(0));
        } else if (getType() == DELETE) {
            response.put("n", new BsonInt32(bulkWriteResult.getDeletedCount()));
        } else if (getType() == UPDATE || getType() == REPLACE) {
            response.put("n", new BsonInt32(bulkWriteResult.getMatchedCount() + bulkWriteResult.getUpserts().size()));
            if (bulkWriteResult.getUpserts().isEmpty()) {
                response.put("updatedExisting", BsonBoolean.TRUE);
            } else {
                response.put("updatedExisting", BsonBoolean.FALSE);
                response.put("upserted", bulkWriteResult.getUpserts().get(0).getId());
            }
        }
    }

    private WriteConcernResult translateBulkWriteResult(final BulkWriteResult bulkWriteResult) {
        return WriteConcernResult.acknowledged(getCount(bulkWriteResult), getUpdatedExisting(bulkWriteResult),
                                               bulkWriteResult.getUpserts().isEmpty()
                                               ? null : bulkWriteResult.getUpserts().get(0).getId());
    }

    protected abstract WriteRequest.Type getType();

    protected abstract int getCount(BulkWriteResult bulkWriteResult);

    protected boolean getUpdatedExisting(final BulkWriteResult bulkWriteResult) {
        return false;
    }

    private BulkWriteError getLastError(final MongoBulkWriteException e) {
        return e.getWriteErrors().isEmpty() ? null : e.getWriteErrors().get(e.getWriteErrors().size() - 1);

    }
}
