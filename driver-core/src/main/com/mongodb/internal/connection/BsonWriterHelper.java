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

import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonMaximumSizeExceededException;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BsonOutput;

import java.util.List;

import static java.lang.String.format;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

final class BsonWriterHelper {
    private static final int DOCUMENT_HEADROOM = 1024 * 16;
    private static final CodecRegistry REGISTRY = fromProviders(new BsonValueCodecProvider());
    private static final EncoderContext ENCODER_CONTEXT = EncoderContext.builder().build();

    static void writeElements(final BsonWriter writer, final List<BsonElement> bsonElements) {
        for (BsonElement bsonElement : bsonElements) {
            writer.writeName(bsonElement.getName());
            getCodec(bsonElement.getValue()).encode(writer, bsonElement.getValue(), ENCODER_CONTEXT);
        }
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
        for (int i = 0; i < payload.getPayload().size(); i++) {
            if (writeDocument(writer, bsonOutput, payloadSettings, payload.getPayload().get(i), messageStartPosition, i + 1,
                    maxSplittableDocumentSize)) {
                payload.setPosition(i + 1);
            } else {
                break;
            }
        }

        if (payload.getPosition() == 0) {
            throw new BsonMaximumSizeExceededException(format("Payload document size is larger than maximum of %d.",
                    payloadSettings.getMaxDocumentSize()));
        }
    }

    private static boolean writeDocument(final BsonWriter writer, final BsonOutput bsonOutput, final MessageSettings settings,
                                         final BsonDocument document, final int messageStartPosition, final int batchItemCount,
                                         final int maxSplittableDocumentSize) {
        int currentPosition = bsonOutput.getPosition();
        getCodec(document).encode(writer, document, ENCODER_CONTEXT);
        int messageSize = bsonOutput.getPosition() - messageStartPosition;
        int documentSize = bsonOutput.getPosition() - currentPosition;
        if (exceedsLimits(settings, messageSize, documentSize, batchItemCount)
                || (batchItemCount > 1 && bsonOutput.getPosition() - messageStartPosition > maxSplittableDocumentSize)) {
            bsonOutput.truncateToPosition(currentPosition);
            return false;
        }
        return true;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Codec<BsonValue> getCodec(final BsonValue bsonValue) {
        return (Codec<BsonValue>) REGISTRY.get(bsonValue.getClass());
    }

    private static MessageSettings getPayloadMessageSettings(final SplittablePayload.Type type, final MessageSettings settings) {
        MessageSettings payloadMessageSettings = settings;
        if (type != SplittablePayload.Type.INSERT) {
            payloadMessageSettings = createMessageSettingsBuilder(settings)
                    .maxDocumentSize(settings.getMaxDocumentSize() + DOCUMENT_HEADROOM)
                    .build();
        }
        return payloadMessageSettings;
    }

    private static MessageSettings getDocumentMessageSettings(final MessageSettings settings) {
        return createMessageSettingsBuilder(settings)
                    .maxMessageSize(settings.getMaxDocumentSize() + DOCUMENT_HEADROOM)
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
