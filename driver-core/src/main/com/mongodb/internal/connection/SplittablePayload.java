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

package com.mongodb.internal.connection;

import com.mongodb.internal.bulk.DeleteRequest;
import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.bulk.WriteRequestWithIndex;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.SplittablePayload.Type.INSERT;
import static com.mongodb.internal.connection.SplittablePayload.Type.REPLACE;
import static com.mongodb.internal.connection.SplittablePayload.Type.UPDATE;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

/**
 * A Splittable payload for write commands.
 *
 * <p>The command will consume as much of the payload as possible. The {@link #hasAnotherSplit()} method will return true if there is
 * another split to consume, {@link #getNextSplit} method will return the next SplittablePayload.</p>
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class SplittablePayload {
    private static final CodecRegistry REGISTRY = fromProviders(new BsonValueCodecProvider());
    private final WriteRequestEncoder writeRequestEncoder = new WriteRequestEncoder();
    private final Type payloadType;
    private final List<WriteRequestWithIndex> writeRequestWithIndexes;
    private final Map<Integer, BsonValue> insertedIds = new HashMap<>();
    private int position = 0;

    /**
     * The type of the payload.
     */
    public enum Type {
        /**
         * An insert.
         */
        INSERT,

        /**
         * An update that uses update operators.
         */
        UPDATE,

        /**
         * An update that replaces the existing document.
         */
        REPLACE,

        /**
         * A delete.
         */
        DELETE
    }

    /**
     * Create a new instance
     *
     * @param payloadType the payload type
     * @param writeRequestWithIndexes the writeRequests
     */
    public SplittablePayload(final Type payloadType, final List<WriteRequestWithIndex> writeRequestWithIndexes) {
        this.payloadType = notNull("batchType", payloadType);
        this.writeRequestWithIndexes = notNull("writeRequests", writeRequestWithIndexes);
    }

    /**
     * @return the payload type
     */
    public Type getPayloadType() {
        return payloadType;
    }

    /**
     * @return the payload name
     */
    public String getPayloadName() {
        if (payloadType == INSERT) {
            return "documents";
        } else if (payloadType == UPDATE || payloadType == REPLACE) {
            return "updates";
        } else {
            return "deletes";
        }
    }

    boolean hasPayload() {
        return writeRequestWithIndexes.size() > 0;
    }

    public int size() {
        return writeRequestWithIndexes.size();
    }

    public Map<Integer, BsonValue> getInsertedIds() {
        return insertedIds;
    }

    /**
     * @return the payload
     */
    public List<BsonDocument> getPayload() {
        return writeRequestWithIndexes.stream().map(wri ->
                    new BsonDocumentWrapper<>(wri, writeRequestEncoder))
                    .collect(Collectors.toList());
    }

    public List<WriteRequestWithIndex> getWriteRequestWithIndexes() {
        return writeRequestWithIndexes;
    }

    /**
     * @return the current position in the payload
     */
    public int getPosition() {
        return position;
    }

    /**
     * Sets the current position in the payload
     * @param position the position
     */
    public void setPosition(final int position) {
        this.position = position;
    }

    /**
     * @return true if there are more values after the current position
     */
    public boolean hasAnotherSplit() {
        return writeRequestWithIndexes.size() > position;
    }

    /**
     * @return a new SplittablePayload containing only the values after the current position.
     */
    public SplittablePayload getNextSplit() {
        isTrue("hasAnotherSplit", hasAnotherSplit());
        List<WriteRequestWithIndex> nextPayLoad = writeRequestWithIndexes.subList(position, writeRequestWithIndexes.size());
        return new SplittablePayload(payloadType, nextPayLoad);
    }

    /**
     * @return true if the writeRequests list is empty
     */
    public boolean isEmpty() {
        return writeRequestWithIndexes.isEmpty();
    }

    class WriteRequestEncoder implements Encoder<WriteRequestWithIndex> {

        WriteRequestEncoder() {
        }

        @Override
        public void encode(final BsonWriter writer, final WriteRequestWithIndex writeRequestWithIndex,
                           final EncoderContext encoderContext) {
            if (writeRequestWithIndex.getType() == WriteRequest.Type.INSERT) {
                InsertRequest insertRequest = (InsertRequest) writeRequestWithIndex.getWriteRequest();
                BsonDocument document = insertRequest.getDocument();

                IdHoldingBsonWriter idHoldingBsonWriter = new IdHoldingBsonWriter(writer);
                getCodec(document).encode(idHoldingBsonWriter, document,
                        EncoderContext.builder().isEncodingCollectibleDocument(true).build());
                insertedIds.put(writeRequestWithIndex.getIndex(), idHoldingBsonWriter.getId());
            } else if (writeRequestWithIndex.getType() == WriteRequest.Type.UPDATE
                    || writeRequestWithIndex.getType() == WriteRequest.Type.REPLACE) {
                UpdateRequest update = (UpdateRequest) writeRequestWithIndex.getWriteRequest();
                writer.writeStartDocument();
                writer.writeName("q");
                getCodec(update.getFilter()).encode(writer, update.getFilter(), EncoderContext.builder().build());

                BsonValue updateValue = update.getUpdateValue();
                if (!updateValue.isDocument() && !updateValue.isArray()) {
                    throw new IllegalArgumentException("Invalid BSON value for an update.");
                }
                if (updateValue.isArray() && updateValue.asArray().isEmpty()) {
                    throw new IllegalArgumentException("Invalid pipeline for an update. The pipeline may not be empty.");
                }

                writer.writeName("u");
                if (updateValue.isDocument()) {
                    FieldTrackingBsonWriter fieldTrackingBsonWriter = new FieldTrackingBsonWriter(writer);
                    getCodec(updateValue.asDocument()).encode(fieldTrackingBsonWriter, updateValue.asDocument(),
                            EncoderContext.builder().build());
                    if (writeRequestWithIndex.getType() == WriteRequest.Type.UPDATE && !fieldTrackingBsonWriter.hasWrittenField()) {
                        throw new IllegalArgumentException("Invalid BSON document for an update. The document may not be empty.");
                    }
                } else if (update.getType() == WriteRequest.Type.UPDATE && updateValue.isArray()) {
                    writer.writeStartArray();
                    for (BsonValue cur : updateValue.asArray()) {
                        getCodec(cur.asDocument()).encode(writer, cur.asDocument(), EncoderContext.builder().build());
                    }
                    writer.writeEndArray();
                }

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
                if (update.getArrayFilters() != null) {
                    writer.writeStartArray("arrayFilters");
                    for (BsonDocument cur: update.getArrayFilters()) {
                        getCodec(cur).encode(writer, cur, EncoderContext.builder().build());
                    }
                    writer.writeEndArray();
                }
                if (update.getHint() != null) {
                    writer.writeName("hint");
                    BsonDocument hint = update.getHint().toBsonDocument(BsonDocument.class, null);
                    getCodec(hint).encode(writer, hint, EncoderContext.builder().build());
                } else if (update.getHintString() != null) {
                    writer.writeString("hint", update.getHintString());
                }
                writer.writeEndDocument();
            } else {
                DeleteRequest deleteRequest = (DeleteRequest) writeRequestWithIndex.getWriteRequest();
                writer.writeStartDocument();
                writer.writeName("q");
                getCodec(deleteRequest.getFilter()).encode(writer, deleteRequest.getFilter(), EncoderContext.builder().build());
                writer.writeInt32("limit", deleteRequest.isMulti() ? 0 : 1);
                if (deleteRequest.getCollation() != null) {
                    writer.writeName("collation");
                    BsonDocument collation = deleteRequest.getCollation().asDocument();
                    getCodec(collation).encode(writer, collation, EncoderContext.builder().build());
                }
                if (deleteRequest.getHint() != null) {
                    writer.writeName("hint");
                    BsonDocument hint = deleteRequest.getHint().toBsonDocument(BsonDocument.class, null);
                    getCodec(hint).encode(writer, hint, EncoderContext.builder().build());
                } else if (deleteRequest.getHintString() != null) {
                    writer.writeString("hint", deleteRequest.getHintString());
                }
                writer.writeEndDocument();
            }
        }

        @Override
        public Class<WriteRequestWithIndex> getEncoderClass() {
            return WriteRequestWithIndex.class;
        }
    }

    @SuppressWarnings("unchecked")
    private static Codec<BsonDocument> getCodec(final BsonDocument document) {
        return (Codec<BsonDocument>) REGISTRY.get(document.getClass());
    }

}
