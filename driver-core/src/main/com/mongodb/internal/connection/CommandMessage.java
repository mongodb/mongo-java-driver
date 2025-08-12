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
import com.mongodb.MongoInternalException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.connection.MessageSequences.EmptyMessageSequences;
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

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.ReadPreference.primaryPreferred;
import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.fail;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ClusterConnectionMode.LOAD_BALANCED;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static com.mongodb.connection.ServerType.SHARD_ROUTER;
import static com.mongodb.connection.ServerType.STANDALONE;
import static com.mongodb.internal.connection.BsonWriterHelper.appendElementsToDocument;
import static com.mongodb.internal.connection.BsonWriterHelper.backpatchLength;
import static com.mongodb.internal.connection.BsonWriterHelper.writeDocumentsOfDualMessageSequences;
import static com.mongodb.internal.connection.BsonWriterHelper.writePayload;
import static com.mongodb.internal.connection.ByteBufBsonDocument.createList;
import static com.mongodb.internal.connection.ByteBufBsonDocument.createOne;
import static com.mongodb.internal.connection.ReadConcernHelper.getReadConcernDocument;
import static com.mongodb.internal.operation.ServerVersionHelper.UNKNOWN_WIRE_VERSION;

/**
 * A command message that uses OP_MSG or OP_QUERY to send the command.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class CommandMessage extends RequestMessage {
    /**
     * Specifies that the `OP_MSG` section payload is a BSON document.
     */
    private static final byte PAYLOAD_TYPE_0_DOCUMENT = 0;
    /**
     * Specifies that the `OP_MSG` section payload is a sequence of BSON documents.
     */
    private static final byte PAYLOAD_TYPE_1_DOCUMENT_SEQUENCE = 1;

    private final BsonDocument command;
    private final FieldNameValidator commandFieldNameValidator;
    private final ReadPreference readPreference;
    private final boolean exhaustAllowed;
    private final MessageSequences sequences;
    private final boolean responseExpected;
    private final String database;
    /**
     * {@code null} iff either {@link #sequences} is not of the {@link DualMessageSequences} type,
     * or it is of that type, but it has not been {@linkplain #encodeMessageBodyWithMetadata(ByteBufferBsonOutput, OperationContext) encoded}.
     */
    @Nullable
    private Boolean dualMessageSequencesRequireResponse;
    private final ClusterConnectionMode clusterConnectionMode;
    private final ServerApi serverApi;

    CommandMessage(final String database, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                   final ReadPreference readPreference, final MessageSettings settings, final ClusterConnectionMode clusterConnectionMode,
                   @Nullable final ServerApi serverApi) {
        this(database, command, commandFieldNameValidator, readPreference, settings, true, EmptyMessageSequences.INSTANCE,
                clusterConnectionMode, serverApi);
    }

    CommandMessage(final String database, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                   final ReadPreference readPreference, final MessageSettings settings, final boolean exhaustAllowed,
                   final ClusterConnectionMode clusterConnectionMode, @Nullable final ServerApi serverApi) {
        this(database, command, commandFieldNameValidator, readPreference, settings, true, exhaustAllowed, EmptyMessageSequences.INSTANCE,
                clusterConnectionMode, serverApi);
    }

    CommandMessage(final String database, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                   final ReadPreference readPreference, final MessageSettings settings, final boolean responseExpected,
                   final MessageSequences sequences,
                   final ClusterConnectionMode clusterConnectionMode, @Nullable final ServerApi serverApi) {
        this(database, command, commandFieldNameValidator, readPreference, settings, responseExpected, false,
                sequences, clusterConnectionMode, serverApi);
    }

    CommandMessage(final String database, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                   final ReadPreference readPreference, final MessageSettings settings,
                   final boolean responseExpected, final boolean exhaustAllowed,
                   final MessageSequences sequences,
                   final ClusterConnectionMode clusterConnectionMode, @Nullable final ServerApi serverApi) {
        super(getOpCode(settings, clusterConnectionMode, serverApi), settings);
        this.database = database;
        this.command = command;
        this.commandFieldNameValidator = commandFieldNameValidator;
        this.readPreference = readPreference;
        this.responseExpected = responseExpected;
        this.dualMessageSequencesRequireResponse = null;
        this.exhaustAllowed = exhaustAllowed;
        this.sequences = sequences;
        this.clusterConnectionMode = notNull("clusterConnectionMode", clusterConnectionMode);
        this.serverApi = serverApi;
        assertTrue(useOpMsg() || responseExpected);
    }

    /**
     * Create a BsonDocument representing the logical document encoded by an OP_MSG.
     * <p>
     * The returned document will contain all the fields from the `PAYLOAD_TYPE_0_DOCUMENT` section, as well as all fields represented by
     * `PAYLOAD_TYPE_1_DOCUMENT_SEQUENCE` sections.
     */
    BsonDocument getCommandDocument(final ByteBufferBsonOutput bsonOutput) {
        List<ByteBuf> byteBuffers = bsonOutput.getByteBuffers();
        try {
            CompositeByteBuf byteBuf = new CompositeByteBuf(byteBuffers);
            try {
                byteBuf.position(getEncodingMetadata().getFirstDocumentPosition());
                ByteBufBsonDocument byteBufBsonDocument = createOne(byteBuf);

                // If true, it means there is at least one `PAYLOAD_TYPE_1_DOCUMENT_SEQUENCE` section in the OP_MSG
                if (byteBuf.hasRemaining()) {
                    BsonDocument commandBsonDocument = byteBufBsonDocument.toBaseBsonDocument();

                    // Each loop iteration processes one Document Sequence
                    // When there are no more bytes remaining, there are no more Document Sequences
                    while (byteBuf.hasRemaining()) {
                        // skip reading the payload type, we know it is `PAYLOAD_TYPE_1`
                        byteBuf.position(byteBuf.position() + 1);
                        int sequenceStart = byteBuf.position();
                        int sequenceSizeInBytes = byteBuf.getInt();
                        int sectionEnd = sequenceStart + sequenceSizeInBytes;

                        String fieldName = getSequenceIdentifier(byteBuf);
                        // If this assertion fires, it means that the driver has started using document sequences for nested fields.  If
                        // so, this method will need to change in order to append the value to the correct nested document.
                        assertFalse(fieldName.contains("."));

                        ByteBuf documentsByteBufSlice = byteBuf.duplicate().limit(sectionEnd);
                        try {
                            commandBsonDocument.append(fieldName, new BsonArray(createList(documentsByteBufSlice)));
                        } finally {
                            documentsByteBufSlice.release();
                        }
                        byteBuf.position(sectionEnd);
                    }
                    return commandBsonDocument;
                } else {
                    return byteBufBsonDocument;
                }
            } finally {
                byteBuf.release();
            }
        } finally {
            byteBuffers.forEach(ByteBuf::release);
        }
    }

    /**
     * Get the field name from a buffer positioned at the start of the document sequence identifier of an OP_MSG Section of type
     * `PAYLOAD_TYPE_1_DOCUMENT_SEQUENCE`.
     * <p>
     * Upon normal completion of the method, the buffer will be positioned at the start of the first BSON object in the sequence.
    */
    private String getSequenceIdentifier(final ByteBuf byteBuf) {
        ByteArrayOutputStream sequenceIdentifierBytes = new ByteArrayOutputStream();
        byte curByte = byteBuf.get();
        while (curByte != 0) {
            sequenceIdentifierBytes.write(curByte);
            curByte = byteBuf.get();
        }
        try {
            return sequenceIdentifierBytes.toString(StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new MongoInternalException("Unexpected exception", e);
        }
    }

    boolean isResponseExpected() {
        if (responseExpected) {
            return true;
        } else {
            if (sequences instanceof SplittablePayload) {
                SplittablePayload payload = (SplittablePayload) sequences;
                return payload.isOrdered() && payload.hasAnotherSplit();
            } else if (sequences instanceof DualMessageSequences) {
                return assertNotNull(dualMessageSequencesRequireResponse);
            } else if (!(sequences instanceof EmptyMessageSequences)) {
                fail(sequences.toString());
            }
            return false;
        }
    }

    @Override
    protected EncodingMetadata encodeMessageBodyWithMetadata(final ByteBufferBsonOutput bsonOutput, final OperationContext operationContext) {
        int commandStartPosition = useOpMsg() ? writeOpMsg(bsonOutput, operationContext) : writeOpQuery(bsonOutput);
        return new EncodingMetadata(commandStartPosition);
    }

    @SuppressWarnings("try")
    private int writeOpMsg(final ByteBufferBsonOutput bsonOutput, final OperationContext operationContext) {
        int messageStartPosition = bsonOutput.getPosition() - MESSAGE_PROLOGUE_LENGTH;
        int flagPosition = bsonOutput.getPosition();
        bsonOutput.writeInt32(0);   // flag bits
        bsonOutput.writeByte(PAYLOAD_TYPE_0_DOCUMENT);
        int commandStartPosition = bsonOutput.getPosition();
        List<BsonElement> extraElements = getExtraElements(operationContext);

        int commandDocumentSizeInBytes = writeDocument(command, bsonOutput, commandFieldNameValidator);
        if (sequences instanceof SplittablePayload) {
            appendElementsToDocument(bsonOutput, commandStartPosition, extraElements);
            SplittablePayload payload = (SplittablePayload) sequences;
            try (FinishOpMsgSectionWithPayloadType1 finishSection = startOpMsgSectionWithPayloadType1(
                    bsonOutput, payload.getPayloadName())) {
                writePayload(
                        new BsonBinaryWriter(bsonOutput, payload.getFieldNameValidator()),
                        bsonOutput, getSettings(), messageStartPosition, payload, getSettings().getMaxDocumentSize());
            }
        } else if (sequences instanceof DualMessageSequences) {
            DualMessageSequences dualMessageSequences = (DualMessageSequences) sequences;
            try (ByteBufferBsonOutput.Branch bsonOutputBranch2 = bsonOutput.branch();
                 ByteBufferBsonOutput.Branch bsonOutputBranch1 = bsonOutput.branch()) {
                DualMessageSequences.EncodeDocumentsResult encodeDocumentsResult;
                try (FinishOpMsgSectionWithPayloadType1 finishSection1 = startOpMsgSectionWithPayloadType1(
                        bsonOutputBranch1, dualMessageSequences.getFirstSequenceId());
                    FinishOpMsgSectionWithPayloadType1 finishSection2 = startOpMsgSectionWithPayloadType1(
                        bsonOutputBranch2, dualMessageSequences.getSecondSequenceId())) {
                    encodeDocumentsResult = writeDocumentsOfDualMessageSequences(
                            dualMessageSequences, commandDocumentSizeInBytes, bsonOutputBranch1,
                            bsonOutputBranch2, getSettings());
                }
                dualMessageSequencesRequireResponse = encodeDocumentsResult.isServerResponseRequired();
                extraElements.addAll(encodeDocumentsResult.getExtraElements());
                appendElementsToDocument(bsonOutput, commandStartPosition, extraElements);
            }
        } else if (sequences instanceof EmptyMessageSequences) {
            appendElementsToDocument(bsonOutput, commandStartPosition, extraElements);
        } else {
            fail(sequences.toString());
        }

        // Write the flag bits
        bsonOutput.writeInt32(flagPosition, getOpMsgFlagBits());
        return commandStartPosition;
    }

    private int writeOpQuery(final ByteBufferBsonOutput bsonOutput) {
        bsonOutput.writeInt32(0);
        bsonOutput.writeCString(new MongoNamespace(getDatabase(), "$cmd").getFullName());
        bsonOutput.writeInt32(0);
        bsonOutput.writeInt32(-1);

        int commandStartPosition = bsonOutput.getPosition();

        List<BsonElement> elements = null;
        if (serverApi != null) {
            elements = new ArrayList<>(3);
            addServerApiElements(elements);
        }
        writeDocument(command, bsonOutput, commandFieldNameValidator);
        appendElementsToDocument(bsonOutput, commandStartPosition, elements);
        return commandStartPosition;
    }

    private int getOpMsgFlagBits() {
        int flagBits = 0;
        if (!isResponseExpected()) {
            flagBits = 1 << 1;
        }
        if (exhaustAllowed) {
            flagBits |= 1 << 16;
        }
        return flagBits;
    }

    private boolean isDirectConnectionToReplicaSetMember() {
        return clusterConnectionMode == SINGLE
                && getSettings().getServerType() != SHARD_ROUTER
                && getSettings().getServerType() != STANDALONE;
    }

    private boolean useOpMsg() {
        return getOpCode().equals(OpCode.OP_MSG);
    }

    private List<BsonElement> getExtraElements(final OperationContext operationContext) {
        SessionContext sessionContext = operationContext.getSessionContext();
        TimeoutContext timeoutContext = operationContext.getTimeoutContext();

        ArrayList<BsonElement> extraElements = new ArrayList<>();
        if (!getSettings().isCryptd()) {
           timeoutContext.runMaxTimeMS(maxTimeMS ->
                   extraElements.add(new BsonElement("maxTimeMS", new BsonInt64(maxTimeMS)))
           );
        }
        extraElements.add(new BsonElement("$db", new BsonString(getDatabase())));
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


    private void addReadConcernDocument(final List<BsonElement> extraElements, final SessionContext sessionContext) {
        BsonDocument readConcernDocument = getReadConcernDocument(sessionContext, getSettings().getMaxWireVersion());
        if (!readConcernDocument.isEmpty()) {
            extraElements.add(new BsonElement("readConcern", readConcernDocument));
        }
    }

    /**
     * @param sequenceId The identifier of the sequence contained in the {@code OP_MSG} section to be written.
     * @see <a href="https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.md">OP_MSG</a>
     */
    private FinishOpMsgSectionWithPayloadType1 startOpMsgSectionWithPayloadType1(final ByteBufferBsonOutput bsonOutput, final String sequenceId) {
        bsonOutput.writeByte(PAYLOAD_TYPE_1_DOCUMENT_SEQUENCE);
        int sequenceStart = bsonOutput.getPosition();
        // size to be patched back later
        bsonOutput.writeInt32(0);
        bsonOutput.writeCString(sequenceId);
        return () -> backpatchLength(sequenceStart, bsonOutput);
    }

    private static OpCode getOpCode(final MessageSettings settings, final ClusterConnectionMode clusterConnectionMode,
            @Nullable final ServerApi serverApi) {
        return isServerVersionKnown(settings) || clusterConnectionMode == LOAD_BALANCED || serverApi != null
                ? OpCode.OP_MSG
                : OpCode.OP_QUERY;
    }

    private static boolean isServerVersionKnown(final MessageSettings settings) {
        return settings.getMaxWireVersion() != UNKNOWN_WIRE_VERSION;
    }

    /**
     * Gets the collection name, which may be null for some message types
     *
     * Gets the database name, which may be null for some message types
     *
     * @return the database name
     */
    public String getDatabase() {
        return database;
    }

    @FunctionalInterface
    private interface FinishOpMsgSectionWithPayloadType1 extends AutoCloseable {
        void close();
    }
}
