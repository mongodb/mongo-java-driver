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

package com.mongodb;

import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.bulk.DeleteRequest;
import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.operation.MixedBulkWriteOperation;
import com.mongodb.internal.operation.WriteOperation;
import com.mongodb.lang.Nullable;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;

import java.util.List;

import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.bulk.WriteRequest.Type.DELETE;
import static com.mongodb.internal.bulk.WriteRequest.Type.INSERT;
import static com.mongodb.internal.bulk.WriteRequest.Type.REPLACE;
import static com.mongodb.internal.bulk.WriteRequest.Type.UPDATE;


/**
 * Operation for bulk writes for the legacy API.
 */
final class LegacyMixedBulkWriteOperation implements WriteOperation<WriteConcernResult> {
    private final MixedBulkWriteOperation wrappedOperation;
    private final WriteRequest.Type type;
    private Boolean bypassDocumentValidation;

    static LegacyMixedBulkWriteOperation createBulkWriteOperationForInsert(final MongoNamespace namespace, final boolean ordered,
            final WriteConcern writeConcern, final boolean retryWrites, final List<InsertRequest> insertRequests) {
        return new LegacyMixedBulkWriteOperation(namespace, ordered, writeConcern, retryWrites, insertRequests, INSERT);
    }

    static LegacyMixedBulkWriteOperation createBulkWriteOperationForUpdate(final MongoNamespace namespace, final boolean ordered,
            final WriteConcern writeConcern, final boolean retryWrites, final List<UpdateRequest> updateRequests) {
        assertTrue(updateRequests.stream().allMatch(updateRequest -> updateRequest.getType() == UPDATE));
        return new LegacyMixedBulkWriteOperation(namespace, ordered, writeConcern, retryWrites, updateRequests, UPDATE);
    }

    static LegacyMixedBulkWriteOperation createBulkWriteOperationForReplace(final MongoNamespace namespace, final boolean ordered,
            final WriteConcern writeConcern, final boolean retryWrites, final List<UpdateRequest> replaceRequests) {
        assertTrue(replaceRequests.stream().allMatch(updateRequest -> updateRequest.getType() == REPLACE));
        return new LegacyMixedBulkWriteOperation(namespace, ordered, writeConcern, retryWrites, replaceRequests, REPLACE);
    }

    static LegacyMixedBulkWriteOperation createBulkWriteOperationForDelete(final MongoNamespace namespace, final boolean ordered,
            final WriteConcern writeConcern, final boolean retryWrites, final List<DeleteRequest> deleteRequests) {
        return new LegacyMixedBulkWriteOperation(namespace, ordered, writeConcern, retryWrites, deleteRequests, DELETE);
    }

    private LegacyMixedBulkWriteOperation(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
            final boolean retryWrites, final List<? extends WriteRequest> writeRequests, final WriteRequest.Type type) {
        notNull("writeRequests", writeRequests);
        isTrueArgument("writeRequests is not an empty list", !writeRequests.isEmpty());
        this.type = type;
        this.wrappedOperation = new MixedBulkWriteOperation(namespace, writeRequests, ordered, writeConcern, retryWrites);
    }

    List<? extends WriteRequest> getWriteRequests() {
        return wrappedOperation.getWriteRequests();
    }

    LegacyMixedBulkWriteOperation bypassDocumentValidation(@Nullable final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    @Override
    public String getCommandName() {
        return wrappedOperation.getCommandName();
    }

    @Override
    public WriteConcernResult execute(final WriteBinding binding) {
        try {
            BulkWriteResult result = wrappedOperation.bypassDocumentValidation(bypassDocumentValidation).execute(binding);
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
        throw new UnsupportedOperationException("This operation is sync only");
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
        if (type == INSERT) {
            response.put("n", new BsonInt32(0));
        } else if (type == DELETE) {
            response.put("n", new BsonInt32(bulkWriteResult.getDeletedCount()));
        } else if (type == UPDATE || type == REPLACE) {
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
        if (type == UPDATE || type == REPLACE) {
            count = bulkWriteResult.getMatchedCount() + bulkWriteResult.getUpserts().size();
        } else if (type == DELETE) {
            count = bulkWriteResult.getDeletedCount();
        }
        return count;
    }

    private boolean getUpdatedExisting(final BulkWriteResult bulkWriteResult) {
        if (type == UPDATE || type == REPLACE) {
            return bulkWriteResult.getMatchedCount() > 0;
        }
        return false;
    }

    @Nullable
    private BulkWriteError getLastError(final MongoBulkWriteException e) {
        return e.getWriteErrors().isEmpty() ? null : e.getWriteErrors().get(e.getWriteErrors().size() - 1);
    }
}
