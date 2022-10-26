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

import com.mongodb.DuplicateKeyException;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernException;
import com.mongodb.WriteConcernResult;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.bulk.WriteRequest;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.bulk.WriteRequest.Type.DELETE;
import static com.mongodb.internal.bulk.WriteRequest.Type.INSERT;
import static com.mongodb.internal.bulk.WriteRequest.Type.REPLACE;
import static com.mongodb.internal.bulk.WriteRequest.Type.UPDATE;


/**
 * Abstract base class for write operations.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public abstract class BaseWriteOperation implements AsyncWriteOperation<WriteConcernResult>, WriteOperation<WriteConcernResult> {
    private final WriteConcern writeConcern;
    private final MongoNamespace namespace;
    private final boolean ordered;
    private final boolean retryWrites;
    private Boolean bypassDocumentValidation;
    private BsonValue comment;

    public BaseWriteOperation(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                              final boolean retryWrites) {
        this.ordered = ordered;
        this.namespace = notNull("namespace", namespace);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.retryWrites = retryWrites;
    }

    protected abstract List<? extends WriteRequest> getWriteRequests();

    protected abstract WriteRequest.Type getType();

    public MongoNamespace getNamespace() {
        return namespace;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    public boolean isOrdered() {
        return ordered;
    }

    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public BaseWriteOperation bypassDocumentValidation(final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    public BsonValue getComment() {
        return comment;
    }

    public BaseWriteOperation comment(final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public WriteConcernResult execute(final WriteBinding binding) {
        try {
            BulkWriteResult result = getMixedBulkOperation().execute(binding);
            if (result.wasAcknowledged()) {
                return translateBulkWriteResult(result);
            } else {
                return WriteConcernResult.unacknowledged();
            }
        } catch (MongoBulkWriteException e) {
            throw convertBulkWriteException(e);
        }
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<WriteConcernResult> callback) {
        getMixedBulkOperation().executeAsync(binding, new SingleResultCallback<BulkWriteResult>() {
                    @Override
                    public void onResult(final BulkWriteResult result, final Throwable t) {
                        if (t != null) {
                            if (t instanceof MongoBulkWriteException) {
                                callback.onResult(null, convertBulkWriteException((MongoBulkWriteException) t));
                            } else {
                                callback.onResult(null, t);
                            }
                        } else if (result.wasAcknowledged()) {
                            callback.onResult(translateBulkWriteResult(result), null);
                        } else {
                            callback.onResult(WriteConcernResult.unacknowledged(), null);
                        }
                    }
                }
        );
    }

    private MixedBulkWriteOperation getMixedBulkOperation() {
        return new MixedBulkWriteOperation(namespace, getWriteRequests(), ordered, writeConcern, retryWrites)
                .bypassDocumentValidation(bypassDocumentValidation)
                .comment(comment);
    }

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

        WriteConcernError writeConcernError = e.getWriteConcernError();
        if (writeConcernError != null) {
            response.putAll(writeConcernError.getDetails());
        }

        BulkWriteError lastError = getLastError(e);
        if (lastError != null) {
            response.put("err", new BsonString(lastError.getMessage()));
            response.put("code", new BsonInt32(lastError.getCode()));
            response.putAll(lastError.getDetails());

        } else if (writeConcernError != null) {
            response.put("err", new BsonString(writeConcernError.getMessage()));
            response.put("code", new BsonInt32(writeConcernError.getCode()));
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

    private int getCount(final BulkWriteResult bulkWriteResult) {


        int count = 0;
        if (getType() == UPDATE || getType() == REPLACE) {
            count = bulkWriteResult.getMatchedCount() + bulkWriteResult.getUpserts().size();
        } else if (getType() == DELETE) {
            count = bulkWriteResult.getDeletedCount();
        }
        return count;
    }

    private boolean getUpdatedExisting(final BulkWriteResult bulkWriteResult) {
        if (getType() == UPDATE) {
            return bulkWriteResult.getMatchedCount() > 0;
        }
        return false;
    }

    private BulkWriteError getLastError(final MongoBulkWriteException e) {
        return e.getWriteErrors().isEmpty() ? null : e.getWriteErrors().get(e.getWriteErrors().size() - 1);

    }
}
