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

import com.mongodb.MongoClientException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBinaryWriter;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.ByteBuf;
import org.bson.FieldNameValidator;
import org.bson.io.BsonOutput;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.ReadPreference.primaryPreferred;
import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ClusterConnectionMode.LOAD_BALANCED;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static com.mongodb.connection.ServerType.SHARD_ROUTER;
import static com.mongodb.connection.ServerType.STANDALONE;
import static com.mongodb.internal.connection.BsonWriterHelper.writePayload;
import static com.mongodb.internal.connection.ByteBufBsonDocument.createList;
import static com.mongodb.internal.connection.ByteBufBsonDocument.createOne;
import static com.mongodb.internal.connection.ReadConcernHelper.getReadConcernDocument;
import static com.mongodb.internal.operation.ServerVersionHelper.FOUR_DOT_TWO_WIRE_VERSION;
import static com.mongodb.internal.operation.ServerVersionHelper.FOUR_DOT_ZERO_WIRE_VERSION;

/**
 * A command message that uses OP_MSG or OP_QUERY to send the command.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class CommandMessage extends RequestMessage {
    private final MongoNamespace namespace;
    private final BsonDocument command;
    private final FieldNameValidator commandFieldNameValidator;
    private final ReadPreference readPreference;
    private final boolean exhaustAllowed;
    private final SplittablePayload payload;
    private final FieldNameValidator payloadFieldNameValidator;
    private final boolean responseExpected;
    private final ClusterConnectionMode clusterConnectionMode;
    private final ServerApi serverApi;

    CommandMessage(final MongoNamespace namespace, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                   final ReadPreference readPreference, final MessageSettings settings, final ClusterConnectionMode clusterConnectionMode,
                   @Nullable final ServerApi serverApi) {
        this(namespace, command, commandFieldNameValidator, readPreference, settings, true, null, null,
                clusterConnectionMode, serverApi);
    }

    CommandMessage(final MongoNamespace namespace, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                   final ReadPreference readPreference, final MessageSettings settings, final boolean exhaustAllowed,
                   final ClusterConnectionMode clusterConnectionMode, @Nullable final ServerApi serverApi) {
        this(namespace, command, commandFieldNameValidator, readPreference, settings, true, exhaustAllowed, null, null,
                clusterConnectionMode, serverApi);
    }

    CommandMessage(final MongoNamespace namespace, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                   final ReadPreference readPreference, final MessageSettings settings, final boolean responseExpected,
                   @Nullable final SplittablePayload payload, @Nullable final FieldNameValidator payloadFieldNameValidator,
                   final ClusterConnectionMode clusterConnectionMode, @Nullable final ServerApi serverApi) {
        this(namespace, command, commandFieldNameValidator, readPreference, settings, responseExpected, false, payload,
                payloadFieldNameValidator, clusterConnectionMode, serverApi);
    }

    CommandMessage(final MongoNamespace namespace, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                   final ReadPreference readPreference, final MessageSettings settings,
                   final boolean responseExpected, final boolean exhaustAllowed,
                   @Nullable final SplittablePayload payload, @Nullable final FieldNameValidator payloadFieldNameValidator,
                   final ClusterConnectionMode clusterConnectionMode, @Nullable final ServerApi serverApi) {
        super(namespace.getFullName(), getOpCode(settings, clusterConnectionMode, serverApi), settings);
        this.namespace = namespace;
        this.command = command;
        this.commandFieldNameValidator = commandFieldNameValidator;
        this.readPreference = readPreference;
        this.responseExpected = responseExpected;
        this.exhaustAllowed = exhaustAllowed;
        this.payload = payload;
        this.payloadFieldNameValidator = payloadFieldNameValidator;
        this.clusterConnectionMode = notNull("clusterConnectionMode", clusterConnectionMode);
        this.serverApi = serverApi;
    }

    // Create a BsonDocument representing the logical document encoded by an OP_MSG.
    // The returned document will contain all the fields, including ones represented by
    // OP_MSG Sections of type Kind 1: Document Sequence
    BsonDocument getCommandDocument(final ByteBufferBsonOutput bsonOutput) {
        List<ByteBuf> duplicateByteBuffers = bsonOutput.getByteBuffers();
        try {
            CompositeByteBuf outputByteBuf = new CompositeByteBuf(duplicateByteBuffers);
            outputByteBuf.position(getEncodingMetadata().getFirstDocumentPosition());
            ByteBufBsonDocument byteBufBsonDocument = createOne(outputByteBuf);

            // If true, it means there is at least one Document Sequence in the OP_MSG
            if (outputByteBuf.hasRemaining()) {
                BsonDocument commandBsonDocument = byteBufBsonDocument.toBaseBsonDocument();

                // Each loop iteration processes one Document Sequence
                // When there are no more bytes remaining, there are no more Document Sequences
                while (outputByteBuf.hasRemaining()) {
                    outputByteBuf.position(outputByteBuf.position() + 1 /* payload type */ + 4 /* payload size */);
                    String payloadName = getPayloadName(outputByteBuf);
                    assertFalse(payloadName.contains("."));
                    commandBsonDocument.append(payloadName, new BsonArray(createList(outputByteBuf)));
                }
                return commandBsonDocument;
            } else {
                return byteBufBsonDocument;
            }
        } finally {
            duplicateByteBuffers.forEach(ByteBuf::release);
        }
    }

    // Get the field name from a ByteBuf positioned at the start of the document sequence identifier of an OP_MSG Section of type
    // Kind 1: Document Sequence.
    // Upon normal completion of the method, the ByteBuf will be positioned at the start of the first BSON object in the sequence.
    private String getPayloadName(final ByteBuf outputByteBuf) {
        List<Byte> payloadNameBytes = new ArrayList<>();
        byte curByte = outputByteBuf.get();
        while (curByte != 0) {
            payloadNameBytes.add(curByte);
            curByte = outputByteBuf.get();
        }

        // Convert List<Byte> to byte[]
        byte[] byteArray = new byte[payloadNameBytes.size()];
        IntStream.range(0, payloadNameBytes.size()).forEachOrdered(i -> byteArray[i] = payloadNameBytes.get(i));
        return new String(byteArray, StandardCharsets.UTF_8);
    }

    boolean isResponseExpected() {
        return !useOpMsg() || requireOpMsgResponse();
    }

    MongoNamespace getNamespace() {
        return namespace;
    }

    @Override
    protected EncodingMetadata encodeMessageBodyWithMetadata(final BsonOutput bsonOutput, final SessionContext sessionContext) {
        int messageStartPosition = bsonOutput.getPosition() - MESSAGE_PROLOGUE_LENGTH;
        int commandStartPosition;
        if (useOpMsg()) {
            int flagPosition = bsonOutput.getPosition();
            bsonOutput.writeInt32(0);   // flag bits
            bsonOutput.writeByte(0);    // payload type
            commandStartPosition = bsonOutput.getPosition();

            addDocument(command, bsonOutput, commandFieldNameValidator, getExtraElements(sessionContext));

            if (payload != null) {
                bsonOutput.writeByte(1);          // payload type
                int payloadBsonOutputStartPosition = bsonOutput.getPosition();
                bsonOutput.writeInt32(0);         // size
                bsonOutput.writeCString(payload.getPayloadName());
                writePayload(new BsonBinaryWriter(bsonOutput, payloadFieldNameValidator), bsonOutput, getSettings(),
                        messageStartPosition, payload, getSettings().getMaxDocumentSize());

                int payloadBsonOutputLength = bsonOutput.getPosition() - payloadBsonOutputStartPosition;
                bsonOutput.writeInt32(payloadBsonOutputStartPosition, payloadBsonOutputLength);
            }

            // Write the flag bits
            bsonOutput.writeInt32(flagPosition, getOpMsgFlagBits());
        } else {
            bsonOutput.writeInt32(0);
            bsonOutput.writeCString(namespace.getFullName());
            bsonOutput.writeInt32(0);
            bsonOutput.writeInt32(-1);

            commandStartPosition = bsonOutput.getPosition();

            List<BsonElement> elements = null;
            if (serverApi != null) {
                elements = new ArrayList<>(3);
                addServerApiElements(elements);
            }
            addDocument(command, bsonOutput, commandFieldNameValidator, elements);
        }
        return new EncodingMetadata(commandStartPosition);
    }

    private int getOpMsgFlagBits() {
        int flagBits = 0;
        if (!requireOpMsgResponse()) {
            flagBits = 1 << 1;
        }
        if (exhaustAllowed) {
            flagBits |= 1 << 16;
        }
        return flagBits;
    }

    private boolean requireOpMsgResponse() {
        if (responseExpected) {
            return true;
        } else {
            return payload != null && payload.hasAnotherSplit();
        }
    }

    private boolean isDirectConnectionToReplicaSetMember() {
        return clusterConnectionMode == SINGLE
                && getSettings().getServerType() != SHARD_ROUTER
                && getSettings().getServerType() != STANDALONE;
    }

    private boolean useOpMsg() {
        return getOpCode().equals(OpCode.OP_MSG);
    }

    private List<BsonElement> getExtraElements(final SessionContext sessionContext) {
        List<BsonElement> extraElements = new ArrayList<>();
        extraElements.add(new BsonElement("$db", new BsonString(new MongoNamespace(getCollectionName()).getDatabaseName())));
        if (sessionContext.getClusterTime() != null) {
            extraElements.add(new BsonElement("$clusterTime", sessionContext.getClusterTime()));
        }
        if (sessionContext.hasSession()) {
            if (!sessionContext.isImplicitSession() && !getSettings().isSessionSupported()) {
                throw new MongoClientException("Attempting to use a ClientSession while connected to a server that doesn't support "
                        + "sessions");
            }
            if (getSettings().isSessionSupported() && responseExpected) {
                extraElements.add(new BsonElement("lsid", sessionContext.getSessionId()));
            }
        }
        boolean firstMessageInTransaction = sessionContext.notifyMessageSent();

        assertFalse(sessionContext.hasActiveTransaction() && sessionContext.isSnapshot());
        if (sessionContext.hasActiveTransaction()) {
            checkServerVersionForTransactionSupport();
            extraElements.add(new BsonElement("txnNumber", new BsonInt64(sessionContext.getTransactionNumber())));
            if (firstMessageInTransaction) {
                extraElements.add(new BsonElement("startTransaction", BsonBoolean.TRUE));
                addReadConcernDocument(extraElements, sessionContext);
            }
            extraElements.add(new BsonElement("autocommit", BsonBoolean.FALSE));
        } else if (sessionContext.isSnapshot()) {
            addReadConcernDocument(extraElements, sessionContext);
        }

        if (serverApi != null) {
            addServerApiElements(extraElements);
        }

        if (readPreference != null) {
            if (!readPreference.equals(primary())) {
                extraElements.add(new BsonElement("$readPreference", readPreference.toDocument()));
            } else if (isDirectConnectionToReplicaSetMember()) {
                extraElements.add(new BsonElement("$readPreference", primaryPreferred().toDocument()));
            }
        }
        return extraElements;
    }

    private void addServerApiElements(final List<BsonElement> extraElements) {
        extraElements.add(new BsonElement("apiVersion", new BsonString(serverApi.getVersion().getValue())));
        if (serverApi.getStrict().isPresent()) {
            extraElements.add(new BsonElement("apiStrict", BsonBoolean.valueOf(serverApi.getStrict().get())));
        }
        if (serverApi.getDeprecationErrors().isPresent()) {
            extraElements.add(new BsonElement("apiDeprecationErrors", BsonBoolean.valueOf(serverApi.getDeprecationErrors().get())));
        }
    }

    private void checkServerVersionForTransactionSupport() {
        if (getSettings().getMaxWireVersion() < FOUR_DOT_TWO_WIRE_VERSION && getSettings().getServerType() == SHARD_ROUTER) {
            throw new MongoClientException("Transactions are not supported by the MongoDB cluster to which this client is connected.");
        }
    }


    private void addReadConcernDocument(final List<BsonElement> extraElements, final SessionContext sessionContext) {
        BsonDocument readConcernDocument = getReadConcernDocument(sessionContext, getSettings().getMaxWireVersion());
        if (!readConcernDocument.isEmpty()) {
            extraElements.add(new BsonElement("readConcern", readConcernDocument));
        }
    }

    private static OpCode getOpCode(final MessageSettings settings, final ClusterConnectionMode clusterConnectionMode,
            @Nullable final ServerApi serverApi) {
        return isServerVersionKnown(settings) || clusterConnectionMode == LOAD_BALANCED || serverApi != null
                ? OpCode.OP_MSG
                : OpCode.OP_QUERY;
    }

    private static boolean isServerVersionKnown(final MessageSettings settings) {
        return settings.getMaxWireVersion() >= FOUR_DOT_ZERO_WIRE_VERSION;
    }
}
