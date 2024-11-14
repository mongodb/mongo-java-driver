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

import com.mongodb.internal.connection.DualMessageSequences.EncodeDocumentsResult;
import com.mongodb.internal.connection.DualMessageSequences.WritersProviderAndLimitsChecker;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonBinaryWriter;
import org.bson.BsonBinaryWriterSettings;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonMaximumSizeExceededException;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.BsonWriterSettings;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BsonOutput;

import java.util.List;

import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.internal.connection.DualMessageSequences.WritersProviderAndLimitsChecker.WriteResult.FAIL_LIMIT_EXCEEDED;
import static com.mongodb.internal.connection.DualMessageSequences.WritersProviderAndLimitsChecker.WriteResult.OK_LIMIT_NOT_REACHED;
import static com.mongodb.internal.connection.DualMessageSequences.WritersProviderAndLimitsChecker.WriteResult.OK_LIMIT_REACHED;
import static com.mongodb.internal.connection.MessageSettings.DOCUMENT_HEADROOM_SIZE;
import static java.lang.String.format;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public final class BsonWriterHelper {
    private static final CodecRegistry REGISTRY = fromProviders(new BsonValueCodecProvider());
    private static final EncoderContext ENCODER_CONTEXT = EncoderContext.builder().build();

    static void appendElementsToDocument(
            final BsonOutput bsonOutputWithDocument,
            final int documentStartPosition,
            @Nullable final List<BsonElement> bsonElements) {
        int bsonDocumentEndingSize = 1;
        int appendFrom = bsonOutputWithDocument.getPosition() - bsonDocumentEndingSize;
        BsonBinaryWriter writer = createBsonBinaryWriter(bsonOutputWithDocument, NoOpFieldNameValidator.INSTANCE, null);
        // change `writer`s state so that we can append elements
        writer.writeStartDocument();
        bsonOutputWithDocument.truncateToPosition(appendFrom);
        if (bsonElements != null) {
            for (BsonElement element : bsonElements) {
                String name = element.getName();
                BsonValue value = element.getValue();
                writer.writeName(name);
                encodeUsingRegistry(writer, value);
            }
        }
        // write the BSON document ending
        bsonOutputWithDocument.writeByte(0);
        backpatchLength(documentStartPosition, bsonOutputWithDocument);
    }

    static void writePayloadArray(final BsonWriter writer, final BsonOutput bsonOutput, final MessageSettings settings,
                                  final int messageStartPosition, final SplittablePayload payload, final int maxSplittableDocumentSize) {
        writer.writeStartArray(payload.getPayloadName());
        writePayload(writer, bsonOutput, getDocumentMessageSettings(settings), messageStartPosition, payload, maxSplittableDocumentSize);
        writer.writeEndArray();
    }

    static void writePayload(final BsonWriter writer, final BsonOutput bsonOutput, final MessageSettings settings,
                             final int messageStartPosition, final SplittablePayload payload, final int maxSplittableDocumentSize) {
        MessageSettings payloadSettings = getPayloadMessageSettings(payload.getPayloadType(), settings);
        List<BsonDocument> payloadDocuments = payload.getPayload();
        for (int i = 0; i < payloadDocuments.size(); i++) {
            if (writeDocument(writer, bsonOutput, payloadSettings, payloadDocuments.get(i), messageStartPosition, i + 1,
                    maxSplittableDocumentSize)) {
                payload.setPosition(i + 1);
            } else {
                break;
            }
        }

        if (payload.getPosition() == 0) {
            throw createBsonMaximumSizeExceededException(payloadSettings.getMaxDocumentSize());
        }
    }

    /**
     * @return See {@link DualMessageSequences#encodeDocuments(WritersProviderAndLimitsChecker)}.
     */
    static EncodeDocumentsResult writeDocumentsOfDualMessageSequences(
            final DualMessageSequences dualMessageSequences,
            final int commandDocumentSizeInBytes,
            final BsonOutput firstOutput,
            final BsonOutput secondOutput,
            final MessageSettings messageSettings) {
        BsonBinaryWriter firstWriter = createBsonBinaryWriter(firstOutput, dualMessageSequences.getFirstFieldNameValidator(), null);
        BsonBinaryWriter secondWriter = createBsonBinaryWriter(secondOutput, dualMessageSequences.getSecondFieldNameValidator(), null);
        // the size of operation-agnostic command fields (a.k.a. extra elements) is counted towards `messageOverheadInBytes`
        int messageOverheadInBytes = 1000;
        int maxSizeInBytes = messageSettings.getMaxMessageSize() - (messageOverheadInBytes + commandDocumentSizeInBytes);
        int firstStart = firstOutput.getPosition();
        int secondStart = secondOutput.getPosition();
        int maxBatchCount = messageSettings.getMaxBatchCount();
        return dualMessageSequences.encodeDocuments(write -> {
            int firstBeforeWritePosition = firstOutput.getPosition();
            int secondBeforeWritePosition = secondOutput.getPosition();
            int batchCountAfterWrite = write.doAndGetBatchCount(firstWriter, secondWriter);
            assertTrue(batchCountAfterWrite <= maxBatchCount);
            int writtenSizeInBytes =
                    firstOutput.getPosition() - firstStart
                    + secondOutput.getPosition() - secondStart;
            if (writtenSizeInBytes < maxSizeInBytes && batchCountAfterWrite < maxBatchCount) {
                return OK_LIMIT_NOT_REACHED;
            } else if (writtenSizeInBytes > maxSizeInBytes) {
                firstOutput.truncateToPosition(firstBeforeWritePosition);
                secondOutput.truncateToPosition(secondBeforeWritePosition);
                if (batchCountAfterWrite == 1) {
                    // we have failed to write a single document
                    throw createBsonMaximumSizeExceededException(messageSettings.getMaxDocumentSize());
                }
                return FAIL_LIMIT_EXCEEDED;
            } else {
                return OK_LIMIT_REACHED;
            }
        });
    }

    /**
     * @param messageSettings Non-{@code null} iff the document size limit must be validated.
     */
    static BsonBinaryWriter createBsonBinaryWriter(
            final BsonOutput out,
            final FieldNameValidator validator,
            @Nullable final MessageSettings messageSettings) {
        return new BsonBinaryWriter(
                new BsonWriterSettings(),
                messageSettings == null
                        ? new BsonBinaryWriterSettings()
                        : new BsonBinaryWriterSettings(messageSettings.getMaxDocumentSize() + DOCUMENT_HEADROOM_SIZE),
                out,
                validator);
    }

    /**
     * Backpatches the document/message/sequence length into the beginning of the document/message/sequence.
     *
     * @param startPosition The start position of the document/message/sequence in {@code bsonOutput}.
     */
    static void backpatchLength(final int startPosition, final BsonOutput bsonOutput) {
        int messageLength = bsonOutput.getPosition() - startPosition;
        bsonOutput.writeInt32(startPosition, messageLength);
    }

    private static BsonMaximumSizeExceededException createBsonMaximumSizeExceededException(final int maxSize) {
        return new BsonMaximumSizeExceededException(format("Payload document size is larger than maximum of %d.", maxSize));
    }

    private static boolean writeDocument(final BsonWriter writer, final BsonOutput bsonOutput, final MessageSettings settings,
                                         final BsonDocument document, final int messageStartPosition, final int batchItemCount,
                                         final int maxSplittableDocumentSize) {
        int currentPosition = bsonOutput.getPosition();
        encodeUsingRegistry(writer, document);
        int messageSize = bsonOutput.getPosition() - messageStartPosition;
        int documentSize = bsonOutput.getPosition() - currentPosition;
        if (exceedsLimits(settings, messageSize, documentSize, batchItemCount)
                || (batchItemCount > 1 && bsonOutput.getPosition() - messageStartPosition > maxSplittableDocumentSize)) {
            bsonOutput.truncateToPosition(currentPosition);
            return false;
        }
        return true;
    }

    static void encodeUsingRegistry(final BsonWriter writer, final BsonValue value) {
        @SuppressWarnings("unchecked")
        Encoder<BsonValue> encoder = (Encoder<BsonValue>) REGISTRY.get(value.getClass());
        encoder.encode(writer, value, ENCODER_CONTEXT);
    }

    private static MessageSettings getPayloadMessageSettings(final SplittablePayload.Type type, final MessageSettings settings) {
        MessageSettings payloadMessageSettings = settings;
        if (type != SplittablePayload.Type.INSERT) {
            payloadMessageSettings = createMessageSettingsBuilder(settings)
                    .maxDocumentSize(settings.getMaxDocumentSize() + DOCUMENT_HEADROOM_SIZE)
                    .build();
        }
        return payloadMessageSettings;
    }

    private static MessageSettings getDocumentMessageSettings(final MessageSettings settings) {
        return createMessageSettingsBuilder(settings)
                    .maxMessageSize(settings.getMaxDocumentSize() + DOCUMENT_HEADROOM_SIZE)
                    .build();
    }

    private static MessageSettings.Builder createMessageSettingsBuilder(final MessageSettings settings) {
        return MessageSettings.builder()
                .maxBatchCount(settings.getMaxBatchCount())
                .maxMessageSize(settings.getMaxMessageSize())
                .maxDocumentSize(settings.getMaxDocumentSize())
                .maxWireVersion(settings.getMaxWireVersion());
    }

    private static boolean exceedsLimits(final MessageSettings settings, final int messageSize, final int documentSize,
                                          final int batchItemCount) {
        if (batchItemCount > settings.getMaxBatchCount()) {
            return true;
        } else if (messageSize > settings.getMaxMessageSize()) {
            return true;
        } else if (documentSize > settings.getMaxDocumentSize()) {
            return true;
        }
        return false;
    }

    private BsonWriterHelper() {
    }
}
