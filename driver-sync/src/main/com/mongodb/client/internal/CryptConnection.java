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

package com.mongodb.client.internal;

import com.mongodb.MongoClientException;
import com.mongodb.ReadPreference;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.MessageSettings;
import com.mongodb.internal.connection.MessageSequences;
import com.mongodb.internal.connection.MessageSequences.EmptyMessageSequences;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.SplittablePayload;
import com.mongodb.internal.connection.SplittablePayloadBsonWriter;
import com.mongodb.internal.time.Timeout;
import com.mongodb.internal.validator.MappedFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonBinaryWriterSettings;
import org.bson.BsonDocument;
import org.bson.BsonWriter;
import org.bson.BsonWriterSettings;
import org.bson.FieldNameValidator;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.RawBsonDocumentCodec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BasicOutputBuffer;

import java.util.HashMap;
import java.util.Map;

import static com.mongodb.assertions.Assertions.fail;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsLessThanVersionFourDotTwo;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public final class CryptConnection implements Connection {
    private static final CodecRegistry REGISTRY = fromProviders(new BsonValueCodecProvider());
    private static final int MAX_SPLITTABLE_DOCUMENT_SIZE = 2097152;

    private final Connection wrapped;
    private final Crypt crypt;

    CryptConnection(final Connection wrapped, final Crypt crypt) {
        this.wrapped = wrapped;
        this.crypt = crypt;
    }

    @Override
    public int getCount() {
        return wrapped.getCount();
    }

    @Override
    public CryptConnection retain() {
        wrapped.retain();
        return this;
    }

    @Override
    public int release() {
        return wrapped.release();
    }

    @Override
    public ConnectionDescription getDescription() {
        return wrapped.getDescription();
    }

    @Nullable
    @Override
    public <T> T command(final String database, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
            @Nullable final ReadPreference readPreference, final Decoder<T> commandResultDecoder,
            final OperationContext operationContext, final boolean responseExpected, final MessageSequences sequences) {

        if (serverIsLessThanVersionFourDotTwo(wrapped.getDescription())) {
            throw new MongoClientException("Auto-encryption requires a minimum MongoDB version of 4.2");
        }

        SplittablePayload payload = null;
        FieldNameValidator payloadFieldNameValidator = null;
        if (sequences instanceof SplittablePayload) {
            payload = (SplittablePayload) sequences;
            payloadFieldNameValidator = payload.getFieldNameValidator();
        } else if (!(sequences instanceof EmptyMessageSequences)) {
            fail(sequences.toString());
        }
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();
        BsonBinaryWriter bsonBinaryWriter = new BsonBinaryWriter(new BsonWriterSettings(),
                new BsonBinaryWriterSettings(getDescription().getMaxDocumentSize()),
                bsonOutput, getFieldNameValidator(payload, commandFieldNameValidator, payloadFieldNameValidator));
        BsonWriter writer = payload == null
                ? bsonBinaryWriter
                : new SplittablePayloadBsonWriter(bsonBinaryWriter, bsonOutput, createSplittablePayloadMessageSettings(), payload,
                MAX_SPLITTABLE_DOCUMENT_SIZE);

        getEncoder(command).encode(writer, command, EncoderContext.builder().build());

        Timeout operationTimeout = operationContext.getTimeoutContext().getTimeout();
        RawBsonDocument encryptedCommand = crypt.encrypt(database,
                new RawBsonDocument(bsonOutput.getInternalBuffer(), 0, bsonOutput.getSize()), operationTimeout);

        RawBsonDocument encryptedResponse = wrapped.command(database, encryptedCommand, commandFieldNameValidator, readPreference,
                new RawBsonDocumentCodec(), operationContext, responseExpected, EmptyMessageSequences.INSTANCE);

        if (encryptedResponse == null) {
            return null;
        }

        RawBsonDocument decryptedResponse = crypt.decrypt(encryptedResponse, operationTimeout);

        BsonBinaryReader reader = new BsonBinaryReader(decryptedResponse.getByteBuffer().asNIO());

        return commandResultDecoder.decode(reader, DecoderContext.builder().build());
    }

    @Nullable
    @Override
    public <T> T command(final String database, final BsonDocument command, final FieldNameValidator fieldNameValidator,
            @Nullable final ReadPreference readPreference, final Decoder<T> commandResultDecoder, final OperationContext operationContext) {
        return command(database, command, fieldNameValidator, readPreference, commandResultDecoder, operationContext, true, EmptyMessageSequences.INSTANCE);
    }

    @SuppressWarnings("unchecked")
    private Codec<BsonDocument> getEncoder(final BsonDocument command) {
        return (Codec<BsonDocument>) REGISTRY.get(command.getClass());
    }

    private FieldNameValidator getFieldNameValidator(@Nullable final SplittablePayload payload,
                                                     final FieldNameValidator commandFieldNameValidator,
                                                     @Nullable final FieldNameValidator payloadFieldNameValidator) {
        if (payload == null) {
            return commandFieldNameValidator;
        }

        Map<String, FieldNameValidator> rootMap = new HashMap<>();
        rootMap.put(payload.getPayloadName(), payloadFieldNameValidator);
        return new MappedFieldNameValidator(commandFieldNameValidator, rootMap);
    }

    private MessageSettings createSplittablePayloadMessageSettings() {
        return MessageSettings.builder()
                .maxBatchCount(getDescription().getMaxBatchCount())
                .maxMessageSize(getDescription().getMaxMessageSize())
                .maxDocumentSize(getDescription().getMaxDocumentSize())
                .build();
    }

    @Override
    public void markAsPinned(final PinningMode pinningMode) {
        wrapped.markAsPinned(pinningMode);
    }
}
