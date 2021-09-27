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
import com.mongodb.MongoClientException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteInsert;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.bulk.DeleteRequest;
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.bulk.WriteRequestWithIndex;
import com.mongodb.internal.connection.BulkWriteBatchCombiner;
import com.mongodb.internal.connection.IndexMap;
import com.mongodb.internal.connection.SplittablePayload;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.validator.MappedFieldNameValidator;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.internal.validator.ReplacingDocumentFieldNameValidator;
import com.mongodb.internal.validator.UpdateFieldNameValidator;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.internal.bulk.WriteRequest.Type.DELETE;
import static com.mongodb.internal.bulk.WriteRequest.Type.INSERT;
import static com.mongodb.internal.bulk.WriteRequest.Type.REPLACE;
import static com.mongodb.internal.bulk.WriteRequest.Type.UPDATE;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.isRetryableWrite;
import static com.mongodb.internal.operation.WriteConcernHelper.createWriteConcernError;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;


public final class BulkWriteBatch {
    private static final CodecRegistry REGISTRY = fromProviders(new BsonValueCodecProvider());
    private static final Decoder<BsonDocument> DECODER = REGISTRY.get(BsonDocument.class);
    private static final FieldNameValidator NO_OP_FIELD_NAME_VALIDATOR = new NoOpFieldNameValidator();

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
    private final SessionContext sessionContext;

    static BulkWriteBatch createBulkWriteBatch(final MongoNamespace namespace,
                                                      final ServerDescription serverDescription,
                                                      final ConnectionDescription connectionDescription,
                                                      final boolean ordered, final WriteConcern writeConcern,
                                                      final Boolean bypassDocumentValidation, final boolean retryWrites,
                                                      final List<? extends WriteRequest> writeRequests,
                                                      final SessionContext sessionContext) {
        if (sessionContext.hasSession() && !sessionContext.isImplicitSession() && !sessionContext.hasActiveTransaction()
                && !writeConcern.isAcknowledged()) {
            throw new MongoClientException("Unacknowledged writes are not supported when using an explicit session");
        }
        boolean canRetryWrites = isRetryableWrite(retryWrites, writeConcern, serverDescription, connectionDescription, sessionContext);
        List<WriteRequestWithIndex> writeRequestsWithIndex = new ArrayList<WriteRequestWithIndex>();
        boolean writeRequestsAreRetryable = true;
        for (int i = 0; i < writeRequests.size(); i++) {
            WriteRequest writeRequest = writeRequests.get(i);
            writeRequestsAreRetryable = writeRequestsAreRetryable && isRetryable(writeRequest);
            writeRequestsWithIndex.add(new WriteRequestWithIndex(writeRequest, i));
        }
        if (canRetryWrites && !writeRequestsAreRetryable) {
            canRetryWrites = false;
            LOGGER.debug("retryWrites set but one or more writeRequests do not support retryable writes");
        }
        return new BulkWriteBatch(namespace, connectionDescription, ordered, writeConcern, bypassDocumentValidation,
                canRetryWrites, new BulkWriteBatchCombiner(connectionDescription.getServerAddress(), ordered, writeConcern),
                writeRequestsWithIndex, sessionContext);
    }

    private BulkWriteBatch(final MongoNamespace namespace, final ConnectionDescription connectionDescription,
                           final boolean ordered, final WriteConcern writeConcern, final Boolean bypassDocumentValidation,
                           final boolean retryWrites, final BulkWriteBatchCombiner bulkWriteBatchCombiner,
                           final List<WriteRequestWithIndex> writeRequestsWithIndices, final SessionContext sessionContext) {
        this.namespace = namespace;
        this.connectionDescription = connectionDescription;
        this.ordered = ordered;
        this.writeConcern = writeConcern;
        this.bypassDocumentValidation = bypassDocumentValidation;
        this.bulkWriteBatchCombiner = bulkWriteBatchCombiner;
        this.batchType = writeRequestsWithIndices.isEmpty() ? INSERT : writeRequestsWithIndices.get(0).getType();
        this.retryWrites = retryWrites;

        List<WriteRequestWithIndex> payloadItems = new ArrayList<>();
        List<WriteRequestWithIndex> unprocessedItems = new ArrayList<WriteRequestWithIndex>();

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
        this.payload = new SplittablePayload(getPayloadType(batchType), payloadItems);
        this.sessionContext = sessionContext;
        this.command = new BsonDocument();

        if (!payloadItems.isEmpty()) {
            command.put(getCommandName(batchType), new BsonString(namespace.getCollectionName()));
            command.put("ordered", new BsonBoolean(ordered));
            if (!writeConcern.isServerDefault() && !sessionContext.hasActiveTransaction()) {
                command.put("writeConcern", writeConcern.asDocument());
            }
            if (bypassDocumentValidation != null) {
                command.put("bypassDocumentValidation", new BsonBoolean(bypassDocumentValidation));
            }
            if (retryWrites) {
                command.put("txnNumber", new BsonInt64(sessionContext.advanceTransactionNumber()));
            }
        }
    }

    private BulkWriteBatch(final MongoNamespace namespace, final ConnectionDescription connectionDescription,
                           final boolean ordered, final WriteConcern writeConcern, final Boolean bypassDocumentValidation,
                           final boolean retryWrites, final BulkWriteBatchCombiner bulkWriteBatchCombiner, final IndexMap indexMap,
                           final WriteRequest.Type batchType, final BsonDocument command, final SplittablePayload payload,
                           final List<WriteRequestWithIndex> unprocessed, final SessionContext sessionContext) {
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
        this.sessionContext = sessionContext;
        if (retryWrites) {
            command.put("txnNumber", new BsonInt64(sessionContext.advanceTransactionNumber()));
        }
        this.command = command;
    }

    void addResult(final BsonDocument result) {
        if (writeConcern.isAcknowledged()) {
            if (hasError(result)) {
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
                    bulkWriteBatchCombiner, nextIndexMap, batchType, command, payload.getNextSplit(), unprocessed, sessionContext);
        } else {
            return new BulkWriteBatch(namespace, connectionDescription, ordered, writeConcern, bypassDocumentValidation, retryWrites,
                    bulkWriteBatchCombiner, unprocessed, sessionContext);
        }
    }

    FieldNameValidator getFieldNameValidator() {
        if (batchType == UPDATE || batchType == REPLACE) {
            Map<String, FieldNameValidator> rootMap = new HashMap<String, FieldNameValidator>();
            if (batchType == WriteRequest.Type.REPLACE) {
                rootMap.put("u", new ReplacingDocumentFieldNameValidator());
            } else {
                rootMap.put("u", new UpdateFieldNameValidator());
            }
            return new MappedFieldNameValidator(NO_OP_FIELD_NAME_VALIDATOR, rootMap);
        } else {
            return NO_OP_FIELD_NAME_VALIDATOR;
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
        if (payload.getPayloadType() == SplittablePayload.Type.INSERT) {

            Stream<WriteRequestWithIndex> writeRequests = payload.getWriteRequestWithIndexes().stream();
            List<Integer> writeErrors = getWriteErrors(result).stream().map(BulkWriteError::getIndex).collect(Collectors.toList());
            if (!writeErrors.isEmpty()) {
                writeRequests = writeRequests.filter(wr -> !writeErrors.contains(wr.getIndex()));
            }
            if (payload.getPosition() < payload.size()) {
                writeRequests = writeRequests.filter(wr -> wr.getIndex() < payload.getPosition());
            }
            return writeRequests
                    .map(wr -> new BulkWriteInsert(wr.getIndex(), payload.getInsertedIds().get(wr.getIndex())))
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
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

    private Integer getModifiedCount(final BsonDocument result) {
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

    @SuppressWarnings("unchecked")
    private List<BulkWriteError> getWriteErrors(final BsonDocument result) {
        List<BulkWriteError> writeErrors = new ArrayList<BulkWriteError>();
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
        if (batchType == WriteRequest.Type.INSERT) {
            return SplittablePayload.Type.INSERT;
        } else if (batchType == WriteRequest.Type.UPDATE) {
            return SplittablePayload.Type.UPDATE;
        } else if (batchType == WriteRequest.Type.REPLACE) {
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
}
