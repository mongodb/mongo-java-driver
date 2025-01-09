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

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteInsert;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.bulk.DeleteRequest;
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.bulk.WriteRequestWithIndex;
import com.mongodb.internal.connection.BulkWriteBatchCombiner;
import com.mongodb.internal.connection.IndexMap;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.SplittablePayload;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.validator.MappedFieldNameValidator;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.internal.validator.ReplacingDocumentFieldNameValidator;
import com.mongodb.internal.validator.UpdateFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Decoder;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.internal.bulk.WriteRequest.Type.DELETE;
import static com.mongodb.internal.bulk.WriteRequest.Type.INSERT;
import static com.mongodb.internal.bulk.WriteRequest.Type.REPLACE;
import static com.mongodb.internal.bulk.WriteRequest.Type.UPDATE;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.CommandOperationHelper.commandWriteConcern;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.isRetryableWrite;
import static com.mongodb.internal.operation.WriteConcernHelper.createWriteConcernError;
import static java.util.Collections.singletonMap;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class BulkWriteBatch {
    private static final CodecRegistry REGISTRY = fromProviders(new BsonValueCodecProvider());
    private static final Decoder<BsonDocument> DECODER = REGISTRY.get(BsonDocument.class);

    private final MongoNamespace namespace;
    private final ConnectionDescription connectionDescription;
    private final boolean ordered;
    private final WriteConcern writeConcern;
    private final Boolean bypassDocumentValidation;
    private final boolean retryWrites;
    private final BulkWriteBatchCombiner bulkWriteBatchCombiner;
    private final IndexMap indexMap;
    private final WriteRequest.Type batchType;
    private final BsonDocument command;
    private final SplittablePayload payload;
    private final List<WriteRequestWithIndex> unprocessed;
    private final OperationContext operationContext;
    private final BsonValue comment;
    private final BsonDocument variables;

    static BulkWriteBatch createBulkWriteBatch(final MongoNamespace namespace,
                                               final ConnectionDescription connectionDescription,
                                                      final boolean ordered, final WriteConcern writeConcern,
                                                      final Boolean bypassDocumentValidation, final boolean retryWrites,
                                                      final List<? extends WriteRequest> writeRequests,
                                                      final OperationContext operationContext,
                                                      @Nullable final BsonValue comment, @Nullable final BsonDocument variables) {
        boolean canRetryWrites = isRetryableWrite(retryWrites, writeConcern, connectionDescription, operationContext.getSessionContext());
        List<WriteRequestWithIndex> writeRequestsWithIndex = new ArrayList<>();
        boolean writeRequestsAreRetryable = true;
        for (int i = 0; i < writeRequests.size(); i++) {
            WriteRequest writeRequest = writeRequests.get(i);
            writeRequestsAreRetryable = writeRequestsAreRetryable && isRetryable(writeRequest);
            writeRequestsWithIndex.add(new WriteRequestWithIndex(writeRequest, i));
        }
        if (canRetryWrites && !writeRequestsAreRetryable) {
            canRetryWrites = false;
            logWriteModelDoesNotSupportRetries();
        }
        return new BulkWriteBatch(namespace, connectionDescription, ordered, writeConcern, bypassDocumentValidation,
                canRetryWrites, new BulkWriteBatchCombiner(connectionDescription.getServerAddress(), ordered, writeConcern),
                writeRequestsWithIndex, operationContext, comment, variables);
    }

    private BulkWriteBatch(final MongoNamespace namespace, final ConnectionDescription connectionDescription,
                           final boolean ordered, final WriteConcern writeConcern, @Nullable final Boolean bypassDocumentValidation,
                           final boolean retryWrites, final BulkWriteBatchCombiner bulkWriteBatchCombiner,
                           final List<WriteRequestWithIndex> writeRequestsWithIndices, final OperationContext operationContext,
                           @Nullable final BsonValue comment, @Nullable final BsonDocument variables) {
        this.namespace = namespace;
        this.connectionDescription = connectionDescription;
        this.ordered = ordered;
        this.writeConcern = writeConcern;
        this.bypassDocumentValidation = bypassDocumentValidation;
        this.bulkWriteBatchCombiner = bulkWriteBatchCombiner;
        this.batchType = writeRequestsWithIndices.isEmpty() ? INSERT : writeRequestsWithIndices.get(0).getType();
        this.retryWrites = retryWrites;

        List<WriteRequestWithIndex> payloadItems = new ArrayList<>();
        List<WriteRequestWithIndex> unprocessedItems = new ArrayList<>();

        IndexMap indexMap = IndexMap.create();
        for (int i = 0; i < writeRequestsWithIndices.size(); i++) {
            WriteRequestWithIndex writeRequestWithIndex = writeRequestsWithIndices.get(i);
            if (writeRequestWithIndex.getType() != batchType) {
                if (ordered) {
                    unprocessedItems.addAll(writeRequestsWithIndices.subList(i, writeRequestsWithIndices.size()));
                    break;
                } else {
                    unprocessedItems.add(writeRequestWithIndex);
                    continue;
                }
            }

            indexMap = indexMap.add(payloadItems.size(), writeRequestWithIndex.getIndex());
            payloadItems.add(writeRequestWithIndex);
        }

        this.indexMap = indexMap;
        this.unprocessed = unprocessedItems;
        this.payload = new SplittablePayload(getPayloadType(batchType), payloadItems, ordered, getFieldNameValidator());
        this.operationContext = operationContext;
        this.comment = comment;
        this.variables = variables;
        this.command = new BsonDocument();

        SessionContext sessionContext = operationContext.getSessionContext();
        if (!payloadItems.isEmpty()) {
            command.put(getCommandName(batchType), new BsonString(namespace.getCollectionName()));
            command.put("ordered", new BsonBoolean(ordered));
            commandWriteConcern(writeConcern, sessionContext).ifPresent(value ->
                    command.put("writeConcern", value.asDocument()));
            if (bypassDocumentValidation != null) {
                command.put("bypassDocumentValidation", new BsonBoolean(bypassDocumentValidation));
            }
            putIfNotNull(command, "comment", comment);
            putIfNotNull(command, "let", variables);
            if (retryWrites) {
                command.put("txnNumber", new BsonInt64(sessionContext.advanceTransactionNumber()));
            }
        }
    }

    private BulkWriteBatch(final MongoNamespace namespace, final ConnectionDescription connectionDescription,
                           final boolean ordered, final WriteConcern writeConcern, final Boolean bypassDocumentValidation,
                           final boolean retryWrites, final BulkWriteBatchCombiner bulkWriteBatchCombiner, final IndexMap indexMap,
                           final WriteRequest.Type batchType, final BsonDocument command, final SplittablePayload payload,
                           final List<WriteRequestWithIndex> unprocessed, final OperationContext operationContext,
                           @Nullable final BsonValue comment, @Nullable final BsonDocument variables) {
        this.namespace = namespace;
        this.connectionDescription = connectionDescription;
        this.ordered = ordered;
        this.writeConcern = writeConcern;
        this.bypassDocumentValidation = bypassDocumentValidation;
        this.bulkWriteBatchCombiner = bulkWriteBatchCombiner;
        this.indexMap = indexMap;
        this.batchType = batchType;
        this.payload = payload;
        this.unprocessed = unprocessed;
        this.retryWrites = retryWrites;
        this.operationContext = operationContext;
        this.comment = comment;
        this.variables = variables;
        if (retryWrites) {
            command.put("txnNumber", new BsonInt64(operationContext.getSessionContext().advanceTransactionNumber()));
        }
        this.command = command;
    }

    void addResult(@Nullable final BsonDocument result) {
        if (writeConcern.isAcknowledged()) {
            if (hasError(assertNotNull(result))) {
                MongoBulkWriteException bulkWriteException = getBulkWriteException(result);
                bulkWriteBatchCombiner.addErrorResult(bulkWriteException, indexMap);
            } else {
                bulkWriteBatchCombiner.addResult(getBulkWriteResult(result));
            }
        }
    }

    boolean getRetryWrites() {
        return retryWrites;
    }

    BsonDocument getCommand() {
        return command;
    }

    SplittablePayload getPayload() {
        return payload;
    }

    Decoder<BsonDocument> getDecoder() {
        return DECODER;
    }

    BulkWriteResult getResult() {
        return bulkWriteBatchCombiner.getResult();
    }

    boolean hasErrors() {
        return bulkWriteBatchCombiner.hasErrors();
    }

    @Nullable
    MongoBulkWriteException getError() {
        return bulkWriteBatchCombiner.getError();
    }

    boolean shouldProcessBatch() {
        return !bulkWriteBatchCombiner.shouldStopSendingMoreBatches() && !payload.isEmpty();
    }

    boolean hasAnotherBatch() {
        return !unprocessed.isEmpty();
    }

    BulkWriteBatch getNextBatch() {
        if (payload.hasAnotherSplit()) {
            IndexMap nextIndexMap = IndexMap.create();
            int newIndex = 0;
            for (int i = payload.getPosition(); i < payload.size(); i++) {
                nextIndexMap = nextIndexMap.add(newIndex, indexMap.map(i));
                newIndex++;
            }


            return new BulkWriteBatch(namespace, connectionDescription, ordered, writeConcern, bypassDocumentValidation, retryWrites,
                    bulkWriteBatchCombiner, nextIndexMap, batchType, command, payload.getNextSplit(), unprocessed, operationContext,
                    comment, variables);
        } else {
            return new BulkWriteBatch(namespace, connectionDescription, ordered, writeConcern, bypassDocumentValidation, retryWrites,
                    bulkWriteBatchCombiner, unprocessed, operationContext, comment, variables);
        }
    }

    private FieldNameValidator getFieldNameValidator() {
        if (batchType == UPDATE || batchType == REPLACE) {
            Map<String, FieldNameValidator> rootMap;
            if (batchType == REPLACE) {
                rootMap = singletonMap("u", ReplacingDocumentFieldNameValidator.INSTANCE);
            } else {
                rootMap = singletonMap("u", new UpdateFieldNameValidator());
            }
            return new MappedFieldNameValidator(NoOpFieldNameValidator.INSTANCE, rootMap);
        } else {
            return NoOpFieldNameValidator.INSTANCE;
        }
    }

    private BulkWriteResult getBulkWriteResult(final BsonDocument result) {
        int count = result.getNumber("n").intValue();
        List<BulkWriteInsert> insertedItems = getInsertedItems(result);
        List<BulkWriteUpsert> upsertedItems = getUpsertedItems(result);
        return BulkWriteResult.acknowledged(batchType, count - upsertedItems.size(), getModifiedCount(result), upsertedItems,
                insertedItems);
    }

    private List<BulkWriteInsert> getInsertedItems(final BsonDocument result) {
        Set<Integer> writeErrors = getWriteErrors(result).stream().map(BulkWriteError::getIndex).collect(Collectors.toSet());
        return payload.getInsertedIds().entrySet().stream()
                .filter(entry -> !writeErrors.contains(entry.getKey()))
                .map(entry -> new BulkWriteInsert(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }


    private List<BulkWriteUpsert> getUpsertedItems(final BsonDocument result) {
        BsonArray upsertedValue = result.getArray("upserted", new BsonArray());
        List<BulkWriteUpsert> bulkWriteUpsertList = new ArrayList<>();
        for (BsonValue upsertedItem : upsertedValue) {
            BsonDocument upsertedItemDocument = (BsonDocument) upsertedItem;
            bulkWriteUpsertList.add(new BulkWriteUpsert(indexMap.map(upsertedItemDocument.getNumber("index").intValue()),
                    upsertedItemDocument.get("_id")));
        }
        return bulkWriteUpsertList;
    }

    private int getModifiedCount(final BsonDocument result) {
        return result.getNumber("nModified", new BsonInt32(0)).intValue();
    }

    private boolean hasError(final BsonDocument result) {
        return result.get("writeErrors") != null || result.get("writeConcernError") != null;
    }

    private MongoBulkWriteException getBulkWriteException(final BsonDocument result) {
        if (!hasError(result)) {
            throw new MongoInternalException("This method should not have been called");
        }

        return new MongoBulkWriteException(getBulkWriteResult(result), getWriteErrors(result),
                getWriteConcernError(result), connectionDescription.getServerAddress(),
                result.getArray("errorLabels", new BsonArray()).stream().map(i -> i.asString().getValue()).collect(Collectors.toSet()));
    }

    private List<BulkWriteError> getWriteErrors(final BsonDocument result) {
        List<BulkWriteError> writeErrors = new ArrayList<>();
        BsonArray writeErrorsDocuments = (BsonArray) result.get("writeErrors");
        if (writeErrorsDocuments != null) {
            for (BsonValue cur : writeErrorsDocuments) {
                BsonDocument curDocument = (BsonDocument) cur;
                writeErrors.add(new BulkWriteError(curDocument.getNumber("code").intValue(),
                        curDocument.getString("errmsg").getValue(),
                        curDocument.getDocument("errInfo", new BsonDocument()),
                        curDocument.getNumber("index").intValue()));
            }
        }
        return writeErrors;
    }

    @Nullable
    private WriteConcernError getWriteConcernError(final BsonDocument result) {
        BsonDocument writeConcernErrorDocument = (BsonDocument) result.get("writeConcernError");
        if (writeConcernErrorDocument == null) {
            return null;
        } else {
            return createWriteConcernError(writeConcernErrorDocument);
        }
    }

    private String getCommandName(final WriteRequest.Type batchType) {
        if (batchType == INSERT) {
            return "insert";
        } else if (batchType == UPDATE || batchType == REPLACE) {
            return "update";
        } else {
            return "delete";
        }
    }

    private SplittablePayload.Type getPayloadType(final WriteRequest.Type batchType) {
        if (batchType == INSERT) {
            return SplittablePayload.Type.INSERT;
        } else if (batchType == UPDATE) {
            return SplittablePayload.Type.UPDATE;
        } else if (batchType == REPLACE) {
            return SplittablePayload.Type.REPLACE;
        } else {
            return SplittablePayload.Type.DELETE;
        }
    }

    private static boolean isRetryable(final WriteRequest writeRequest) {
        if (writeRequest.getType() == UPDATE || writeRequest.getType() == REPLACE) {
            return !((UpdateRequest) writeRequest).isMulti();
        } else if (writeRequest.getType() == DELETE) {
            return !((DeleteRequest) writeRequest).isMulti();
        }
        return true;
    }

    static void logWriteModelDoesNotSupportRetries() {
        LOGGER.debug("retryWrites set but one or more writeRequests do not support retryable writes");
    }
}
