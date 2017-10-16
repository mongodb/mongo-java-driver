/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.operation;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.UpdateRequest;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.bulk.WriteRequest;
import com.mongodb.connection.BulkWriteBatchCombiner;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.SplittablePayload;
import com.mongodb.internal.connection.IndexMap;
import com.mongodb.internal.validator.CollectibleDocumentFieldNameValidator;
import com.mongodb.internal.validator.MappedFieldNameValidator;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.internal.validator.UpdateFieldNameValidator;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonInt32;
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.bulk.WriteRequest.Type.INSERT;
import static com.mongodb.bulk.WriteRequest.Type.REPLACE;
import static com.mongodb.bulk.WriteRequest.Type.UPDATE;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

final class BulkWriteBatch {
    private static final CodecRegistry REGISTRY = fromProviders(new BsonValueCodecProvider());
    private static final Decoder<BsonDocument> DECODER = REGISTRY.get(BsonDocument.class);
    private static final FieldNameValidator NO_OP_FIELD_NAME_VALIDATOR = new NoOpFieldNameValidator();
    private static final WriteRequestEncoder WRITE_REQUEST_ENCODER = new WriteRequestEncoder();

    private final MongoNamespace namespace;
    private final ConnectionDescription connectionDescription;
    private final boolean ordered;
    private final WriteConcern writeConcern;
    private final Boolean bypassDocumentValidation;
    private final BulkWriteBatchCombiner bulkWriteBatchCombiner;
    private final IndexMap indexMap;
    private final WriteRequest.Type batchType;
    private final BsonDocument command;
    private final SplittablePayload payload;
    private final List<WriteRequestWithIndex> unprocessed;

    public static BulkWriteBatch createBulkWriteBatch(final MongoNamespace namespace, final ConnectionDescription connectionDescription,
                                                      final boolean ordered, final WriteConcern writeConcern,
                                                      final Boolean bypassDocumentValidation,
                                                      final List<? extends WriteRequest> writeRequests) {
        List<WriteRequestWithIndex> writeRequestsWithIndex = new ArrayList<WriteRequestWithIndex>();
        for (int i = 0; i < writeRequests.size(); i++) {
            writeRequestsWithIndex.add(new WriteRequestWithIndex(writeRequests.get(i), i));
        }

        return new BulkWriteBatch(namespace, connectionDescription, ordered, writeConcern, bypassDocumentValidation,
                new BulkWriteBatchCombiner(connectionDescription.getServerAddress(), ordered, writeConcern), writeRequestsWithIndex);
    }

    private BulkWriteBatch(final MongoNamespace namespace, final ConnectionDescription connectionDescription,
                           final boolean ordered, final WriteConcern writeConcern, final Boolean bypassDocumentValidation,
                           final BulkWriteBatchCombiner bulkWriteBatchCombiner,
                           final List<WriteRequestWithIndex> writeRequestsWithIndices) {
        this.namespace = namespace;
        this.connectionDescription = connectionDescription;
        this.ordered = ordered;
        this.writeConcern = writeConcern;
        this.bypassDocumentValidation = bypassDocumentValidation;
        this.bulkWriteBatchCombiner = bulkWriteBatchCombiner;
        this.batchType = writeRequestsWithIndices.isEmpty() ? INSERT : writeRequestsWithIndices.get(0).writeRequest.getType();
        this.command = new BsonDocument();

        command.put(getCommandName(batchType), new BsonString(namespace.getCollectionName()));
        command.put("ordered", new BsonBoolean(ordered));
        if (!writeConcern.isServerDefault()) {
            command.put("writeConcern", writeConcern.asDocument());
        }
        if (bypassDocumentValidation != null) {
            command.put("bypassDocumentValidation", new BsonBoolean(bypassDocumentValidation));
        }

        List<BsonDocument> payloadItems = new ArrayList<BsonDocument>();
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

            indexMap = indexMap.add(payloadItems.size(), writeRequestWithIndex.index);
            payloadItems.add(new BsonDocumentWrapper<WriteRequest>(writeRequestWithIndex.writeRequest, WRITE_REQUEST_ENCODER));
        }

        this.indexMap = indexMap;
        this.unprocessed = unprocessedItems;
        this.payload = new SplittablePayload(getPayloadType(batchType), payloadItems);
    }

    private BulkWriteBatch(final MongoNamespace namespace, final ConnectionDescription connectionDescription,
                           final boolean ordered, final WriteConcern writeConcern, final Boolean bypassDocumentValidation,
                           final BulkWriteBatchCombiner bulkWriteBatchCombiner, final IndexMap indexMap, final WriteRequest.Type batchType,
                           final BsonDocument command, final SplittablePayload payload, final List<WriteRequestWithIndex> unprocessed) {
        this.namespace = namespace;
        this.connectionDescription = connectionDescription;
        this.ordered = ordered;
        this.writeConcern = writeConcern;
        this.bypassDocumentValidation = bypassDocumentValidation;
        this.bulkWriteBatchCombiner = bulkWriteBatchCombiner;
        this.indexMap = indexMap;
        this.batchType = batchType;
        this.command = command;
        this.payload = payload;
        this.unprocessed = unprocessed;
    }

    public void addResult(final BsonDocument result) {
        if (writeConcern.isAcknowledged()) {
            if (hasError(result)) {
                MongoBulkWriteException bulkWriteException = getBulkWriteException(result);
                bulkWriteBatchCombiner.addErrorResult(bulkWriteException, indexMap);
            } else {
                bulkWriteBatchCombiner.addResult(getBulkWriteResult(result), indexMap);
            }
        }
    }

    public BsonDocument getCommand() {
        return command;
    }

    public SplittablePayload getPayload() {
        return payload;
    }

    public Decoder<BsonDocument> getDecoder() {
        return DECODER;
    }

    public BulkWriteResult getResult() {
        return bulkWriteBatchCombiner.getResult();
    }

    public boolean hasErrors() {
        return bulkWriteBatchCombiner.hasErrors();
    }

    public MongoBulkWriteException getError() {
        return bulkWriteBatchCombiner.getError();
    }

    public boolean shouldProcessBatch() {
        return !bulkWriteBatchCombiner.shouldStopSendingMoreBatches() && !payload.isEmpty();
    }

    public boolean hasAnotherBatch() {
        return !unprocessed.isEmpty();
    }

    public BulkWriteBatch getNextBatch() {
        if (payload.hasAnotherSplit()) {
            IndexMap nextIndexMap = IndexMap.create();
            int newIndex = 0;
            for (int i = payload.getPosition(); i < payload.getPayload().size(); i++) {
                nextIndexMap = nextIndexMap.add(newIndex, indexMap.map(i));
                newIndex++;
            }

            return new BulkWriteBatch(namespace, connectionDescription, ordered, writeConcern, bypassDocumentValidation,
                    bulkWriteBatchCombiner, nextIndexMap, batchType, command, payload.getNextSplit(), unprocessed);
        } else {
            return new BulkWriteBatch(namespace, connectionDescription, ordered, writeConcern, bypassDocumentValidation,
                    bulkWriteBatchCombiner, unprocessed);
        }
    }

    public FieldNameValidator getFieldNameValidator() {
        if (batchType == INSERT) {
            return new CollectibleDocumentFieldNameValidator();
        } else if (batchType == UPDATE || batchType == REPLACE) {
            Map<String, FieldNameValidator> rootMap = new HashMap<String, FieldNameValidator>();
            if (batchType == WriteRequest.Type.REPLACE) {
                rootMap.put("u", new CollectibleDocumentFieldNameValidator());
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
        List<BulkWriteUpsert> upsertedItems = getUpsertedItems(result);
        return BulkWriteResult.acknowledged(batchType, count - upsertedItems.size(), getModifiedCount(result), upsertedItems);
    }

    @SuppressWarnings("unchecked")
    private List<BulkWriteUpsert> getUpsertedItems(final BsonDocument result) {
        BsonArray upsertedValue = result.getArray("upserted", new BsonArray());
        List<BulkWriteUpsert> bulkWriteUpsertList = new ArrayList<BulkWriteUpsert>();
        for (BsonValue upsertedItem : upsertedValue) {
            BsonDocument upsertedItemDocument = (BsonDocument) upsertedItem;
            bulkWriteUpsertList.add(new BulkWriteUpsert(upsertedItemDocument.getNumber("index").intValue(),
                    upsertedItemDocument.get("_id")));
        }
        return bulkWriteUpsertList;
    }

    private Integer getModifiedCount(final BsonDocument result) {
        BsonNumber modifiedCount = result.getNumber("nModified",
                (batchType == UPDATE || batchType == REPLACE) ? null : new BsonInt32(0));
        return modifiedCount == null ? null : modifiedCount.intValue();
    }

    private boolean hasError(final BsonDocument result) {
        return result.get("writeErrors") != null || result.get("writeConcernError") != null;
    }

    private MongoBulkWriteException getBulkWriteException(final BsonDocument result) {
        if (!hasError(result)) {
            throw new MongoInternalException("This method should not have been called");
        }
        return new MongoBulkWriteException(getBulkWriteResult(result), getWriteErrors(result), getWriteConcernError(result),
                connectionDescription.getServerAddress());
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
            return new WriteConcernError(writeConcernErrorDocument.getNumber("code").intValue(),
                    writeConcernErrorDocument.getString("errmsg").getValue(),
                    writeConcernErrorDocument.getDocument("errInfo", new BsonDocument()));
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

    @SuppressWarnings("unchecked")
    private static Codec<BsonDocument> getCodec(final BsonDocument document) {
        return (Codec<BsonDocument>) REGISTRY.get(document.getClass());
    }

    static class WriteRequestEncoder implements Encoder<WriteRequest> {

        WriteRequestEncoder() {
        }

        @Override
        @SuppressWarnings("unchecked")
        public void encode(final BsonWriter writer, final WriteRequest writeRequest, final EncoderContext encoderContext) {
            if (writeRequest.getType() == INSERT) {
                BsonDocument document = ((InsertRequest) writeRequest).getDocument();
                getCodec(document).encode(writer, document, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
            } else if (writeRequest.getType() == UPDATE || writeRequest.getType() == REPLACE) {
                UpdateRequest update = (UpdateRequest) writeRequest;
                writer.writeStartDocument();
                writer.writeName("q");
                getCodec(update.getFilter()).encode(writer, update.getFilter(), EncoderContext.builder().build());
                writer.writeName("u");

                if (update.getType() == WriteRequest.Type.UPDATE && update.getUpdate().isEmpty()) {
                    throw new IllegalArgumentException("Invalid BSON document for an update");
                }
                getCodec(update.getUpdate()).encode(writer, update.getUpdate(), EncoderContext.builder().build());

                if (update.isMulti()) {
                    writer.writeBoolean("multi", update.isMulti());
                }
                if (update.isUpsert()) {
                    writer.writeBoolean("upsert", update.isUpsert());
                }
                if (update.getCollation() != null) {
                    writer.writeName("collation");
                    BsonDocument collation = update.getCollation().asDocument();
                    getCodec(collation).encode(writer, collation, EncoderContext.builder().build());
                }
                writer.writeEndDocument();
            } else {
                DeleteRequest deleteRequest = (DeleteRequest) writeRequest;
                writer.writeStartDocument();
                writer.writeName("q");
                getCodec(deleteRequest.getFilter()).encode(writer, deleteRequest.getFilter(), EncoderContext.builder().build());
                writer.writeInt32("limit", deleteRequest.isMulti() ? 0 : 1);
                if (deleteRequest.getCollation() != null) {
                    writer.writeName("collation");
                    BsonDocument collation = deleteRequest.getCollation().asDocument();
                    getCodec(collation).encode(writer, collation, EncoderContext.builder().build());
                }
                writer.writeEndDocument();
            }
        }

        @Override
        public Class<WriteRequest> getEncoderClass() {
            return WriteRequest.class;
        }
    }

    static class WriteRequestWithIndex {
        private final int index;
        private final WriteRequest writeRequest;

        WriteRequestWithIndex(final WriteRequest writeRequest, final int index) {
            this.writeRequest = writeRequest;
            this.index = index;
        }

        WriteRequest.Type getType() {
            return writeRequest.getType();
        }
    }

}
