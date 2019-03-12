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
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcernResult;
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.UpdateRequest;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.QueryResult;
import com.mongodb.connection.SplittablePayload;
import com.mongodb.internal.connection.MessageSettings;
import com.mongodb.internal.connection.SplittablePayloadBsonWriter;
import com.mongodb.internal.validator.MappedFieldNameValidator;
import com.mongodb.session.SessionContext;
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
import java.util.List;
import java.util.Map;

import static com.mongodb.internal.operation.ServerVersionHelper.serverIsLessThanVersionFourDotTwo;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

// because this class implements deprecated methods
@SuppressWarnings("deprecation")
class CryptConnection implements Connection {
    private static final CodecRegistry REGISTRY = fromProviders(new BsonValueCodecProvider());

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
    public void release() {
        wrapped.release();
    }

    @Override
    public ConnectionDescription getDescription() {
        return wrapped.getDescription();
    }

    @Override
    public <T> T command(final String database, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                         final ReadPreference readPreference, final Decoder<T> commandResultDecoder, final SessionContext sessionContext,
                         final boolean responseExpected, final SplittablePayload payload,
                         final FieldNameValidator payloadFieldNameValidator) {

        if (serverIsLessThanVersionFourDotTwo(wrapped.getDescription())) {
            throw new MongoClientException("Auto-encryption requires a minimum MongoDB version of 4.2");
        }

        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();
        BsonBinaryWriter bsonBinaryWriter = new BsonBinaryWriter(createBsonWriterSettings(), createBsonBinaryWriterSettings(),
                bsonOutput, getFieldNameValidator(payload, commandFieldNameValidator, payloadFieldNameValidator));
        BsonWriter writer = payload == null
                ? bsonBinaryWriter
                : new SplittablePayloadBsonWriter(bsonBinaryWriter, bsonOutput, createSplittablePayloadMessageSettings(), payload);

        getEncoder(command).encode(writer, command, EncoderContext.builder().build());

        RawBsonDocument encryptedCommand = crypt.encrypt(database,
                new RawBsonDocument(bsonOutput.getInternalBuffer(), 0, bsonOutput.getSize()));

        RawBsonDocument encryptedResponse = wrapped.command(database, encryptedCommand, commandFieldNameValidator, readPreference,
                new RawBsonDocumentCodec(), sessionContext, responseExpected, null, null);

        RawBsonDocument decryptedResponse = crypt.decrypt(encryptedResponse);

        BsonBinaryReader reader = new BsonBinaryReader(decryptedResponse.getByteBuffer().asNIO());

        return commandResultDecoder.decode(reader, DecoderContext.builder().build());
    }

    @Override
    public <T> T command(final String database, final BsonDocument command, final FieldNameValidator fieldNameValidator,
                         final ReadPreference readPreference, final Decoder<T> commandResultDecoder, final SessionContext sessionContext) {
        return command(database, command, fieldNameValidator, readPreference, commandResultDecoder, sessionContext, true, null, null);
    }

    @SuppressWarnings("unchecked")
    private Codec<BsonDocument> getEncoder(final BsonDocument command) {
        return (Codec<BsonDocument>) REGISTRY.get(command.getClass());
    }

    private FieldNameValidator getFieldNameValidator(final SplittablePayload payload,
                                                     final FieldNameValidator commandFieldNameValidator,
                                                     final FieldNameValidator payloadFieldNameValidator) {
        if (payload == null) {
            return commandFieldNameValidator;
        }

        Map<String, FieldNameValidator> rootMap = new HashMap<String, FieldNameValidator>();
        rootMap.put(payload.getPayloadName(), payloadFieldNameValidator);
        return new MappedFieldNameValidator(commandFieldNameValidator, rootMap);
    }

    private BsonWriterSettings createBsonWriterSettings() {
        return new BsonWriterSettings();
    }

    // TODO
    // Currently these settings will allow a split point that is potentially too large after encryption occurs. Need to find a way
    // to allow at least one document to be included in the split even if it is equal in size to maxDocumentSize, but still split at a
    // much smaller document size for the command document itself.
    private BsonBinaryWriterSettings createBsonBinaryWriterSettings() {
        ConnectionDescription connectionDescription = wrapped.getDescription();
        return new BsonBinaryWriterSettings(connectionDescription.getMaxDocumentSize());
    }

    private MessageSettings createSplittablePayloadMessageSettings() {
        ConnectionDescription connectionDescription = wrapped.getDescription();
        return MessageSettings.builder()
                .maxBatchCount(connectionDescription.getMaxBatchCount())
                .maxMessageSize(connectionDescription.getMaxDocumentSize())
                .maxDocumentSize(connectionDescription.getMaxDocumentSize())
                .build();
    }


    // UNSUPPORTED METHODS for encryption/decryption

    @Override
    public <T> T command(final String database, final BsonDocument command, final boolean slaveOk,
                         final FieldNameValidator fieldNameValidator, final Decoder<T> commandResultDecoder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public WriteConcernResult insert(final MongoNamespace namespace, final boolean ordered, final InsertRequest insertRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public WriteConcernResult update(final MongoNamespace namespace, final boolean ordered, final UpdateRequest updateRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public WriteConcernResult delete(final MongoNamespace namespace, final boolean ordered, final DeleteRequest deleteRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> QueryResult<T> query(final MongoNamespace namespace, final BsonDocument queryDocument, final BsonDocument fields,
                                    final int numberToReturn, final int skip, final boolean slaveOk, final boolean tailableCursor,
                                    final boolean awaitData, final boolean noCursorTimeout, final boolean partial,
                                    final boolean oplogReplay, final Decoder<T> resultDecoder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> QueryResult<T> query(final MongoNamespace namespace, final BsonDocument queryDocument, final BsonDocument fields,
                                    final int skip, final int limit, final int batchSize, final boolean slaveOk,
                                    final boolean tailableCursor, final boolean awaitData, final boolean noCursorTimeout,
                                    final boolean partial, final boolean oplogReplay, final Decoder<T> resultDecoder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> QueryResult<T> getMore(final MongoNamespace namespace, final long cursorId, final int numberToReturn,
                                      final Decoder<T> resultDecoder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void killCursor(final List<Long> cursors) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void killCursor(final MongoNamespace namespace, final List<Long> cursors) {
        throw new UnsupportedOperationException();
    }

}
