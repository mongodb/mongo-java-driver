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
import com.mongodb.internal.connection.OpMsgSequences.EmptyOpMsgSequences;
import com.mongodb.internal.operation.ClientBulkWriteOperation.ClientBulkWriteCommand;
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
import java.util.function.Supplier;

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
import static com.mongodb.internal.connection.BsonWriterHelper.writeOpsAndNsInfo;
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
    private static final String TXN_NUMBER_KEY = "txnNumber";

    private final MongoNamespace namespace;
    private final BsonDocument command;
    private final FieldNameValidator commandFieldNameValidator;
    private final ReadPreference readPreference;
    private final boolean exhaustAllowed;
    private final OpMsgSequences sequences;
    private final boolean responseExpected;
    /**
     * {@code null} iff either {@link #sequences} is not of the {@link ClientBulkWriteCommand.OpsAndNsInfo} type,
     * or it is of that type, but it has not been {@linkplain #encodeMessageBodyWithMetadata(ByteBufferBsonOutput, OperationContext) encoded}.
     */
    @Nullable
    private Boolean opsAndNsInfoRequireResponse;
    private final ClusterConnectionMode clusterConnectionMode;
    private final ServerApi serverApi;

    CommandMessage(final MongoNamespace namespace, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                   final ReadPreference readPreference, final MessageSettings settings, final ClusterConnectionMode clusterConnectionMode,
                   @Nullable final ServerApi serverApi) {
        this(namespace, command, commandFieldNameValidator, readPreference, settings, true, EmptyOpMsgSequences.INSTANCE,
                clusterConnectionMode, serverApi);
    }

    CommandMessage(final MongoNamespace namespace, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                   final ReadPreference readPreference, final MessageSettings settings, final boolean exhaustAllowed,
                   final ClusterConnectionMode clusterConnectionMode, @Nullable final ServerApi serverApi) {
        this(namespace, command, commandFieldNameValidator, readPreference, settings, true, exhaustAllowed, EmptyOpMsgSequences.INSTANCE,
                clusterConnectionMode, serverApi);
    }

    CommandMessage(final MongoNamespace namespace, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                   final ReadPreference readPreference, final MessageSettings settings, final boolean responseExpected,
                   final OpMsgSequences sequences,
                   final ClusterConnectionMode clusterConnectionMode, @Nullable final ServerApi serverApi) {
        this(namespace, command, commandFieldNameValidator, readPreference, settings, responseExpected, false,
                sequences, clusterConnectionMode, serverApi);
    }

    CommandMessage(final MongoNamespace namespace, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                   final ReadPreference readPreference, final MessageSettings settings,
                   final boolean responseExpected, final boolean exhaustAllowed,
                   final OpMsgSequences sequences,
                   final ClusterConnectionMode clusterConnectionMode, @Nullable final ServerApi serverApi) {
        super(namespace.getFullName(), getOpCode(settings, clusterConnectionMode, serverApi), settings);
        this.namespace = namespace;
        this.command = command;
        this.commandFieldNameValidator = commandFieldNameValidator;
        this.readPreference = readPreference;
        this.responseExpected = responseExpected;
        opsAndNsInfoRequireResponse = null;
        this.exhaustAllowed = exhaustAllowed;
        this.sequences = sequences;
        this.clusterConnectionMode = notNull("clusterConnectionMode", clusterConnectionMode);
        this.serverApi = serverApi;
        assertTrue(useOpMsg() || responseExpected);
    }

    /**
     * Create a BsonDocument representing the logical document encoded by an OP_MSG.
     * <p>
     * The returned document will contain all the fields from the Body (Kind 0) Section, as well as all fields represented by
     * OP_MSG Document Sequence (Kind 1) Sections.
     */
    BsonDocument getCommandDocument(final ByteBufferBsonOutput bsonOutput) {
        List<ByteBuf> byteBuffers = bsonOutput.getByteBuffers();
        try {
            CompositeByteBuf byteBuf = new CompositeByteBuf(byteBuffers);
            try {
                byteBuf.position(getEncodingMetadata().getFirstDocumentPosition());
                ByteBufBsonDocument byteBufBsonDocument = createOne(byteBuf);

                // If true, it means there is at least one Kind 1:Document Sequence in the OP_MSG
                if (byteBuf.hasRemaining()) {
                    BsonDocument commandBsonDocument = byteBufBsonDocument.toBaseBsonDocument();

                    // Each loop iteration processes one Document Sequence
                    // When there are no more bytes remaining, there are no more Document Sequences
                    while (byteBuf.hasRemaining()) {
                        // skip reading the payload type, we know it is 1
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
     * Document Sequence (Kind 1).
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
            if (sequences instanceof ValidatableSplittablePayload) {
                ValidatableSplittablePayload validatableSplittablePayload = (ValidatableSplittablePayload) sequences;
                SplittablePayload payload = validatableSplittablePayload.getSplittablePayload();
                return payload.isOrdered() && payload.hasAnotherSplit();
            } else if (sequences instanceof ClientBulkWriteCommand.OpsAndNsInfo) {
                return assertNotNull(opsAndNsInfoRequireResponse);
            } else if (!(sequences instanceof EmptyOpMsgSequences)) {
                fail(sequences.toString());
            }
            return false;
        }
    }

    MongoNamespace getNamespace() {
        return namespace;
    }

    @Override
    protected EncodingMetadata encodeMessageBodyWithMetadata(final ByteBufferBsonOutput bsonOutput, final OperationContext operationContext) {
        int messageStartPosition = bsonOutput.getPosition() - MESSAGE_PROLOGUE_LENGTH;
        int commandStartPosition;
        if (useOpMsg()) {
            int flagPosition = bsonOutput.getPosition();
            bsonOutput.writeInt32(0);   // flag bits
            bsonOutput.writeByte(0);    // payload type
            commandStartPosition = bsonOutput.getPosition();
            ArrayList<BsonElement> extraElements = getExtraElements(operationContext);
            // `OpsAndNsInfo` requires validation only if no response is expected, otherwise we must rely on the server validation
            boolean validateDocumentSizeLimits = !(sequences instanceof ClientBulkWriteCommand.OpsAndNsInfo) || !responseExpected;

            int commandDocumentSizeInBytes = writeDocument(command, bsonOutput, commandFieldNameValidator, validateDocumentSizeLimits);
            if (sequences instanceof ValidatableSplittablePayload) {
                appendElementsToDocument(bsonOutput, commandStartPosition, extraElements);
                ValidatableSplittablePayload validatableSplittablePayload = (ValidatableSplittablePayload) sequences;
                SplittablePayload payload = validatableSplittablePayload.getSplittablePayload();
                writeOpMsgSectionWithPayloadType1(bsonOutput, payload.getPayloadName(), () -> {
                        writePayload(
                                new BsonBinaryWriter(bsonOutput, validatableSplittablePayload.getFieldNameValidator()),
                                bsonOutput, getSettings(), messageStartPosition, payload, getSettings().getMaxDocumentSize()
                        );
                        return null;
                });
            } else if (sequences instanceof ClientBulkWriteCommand.OpsAndNsInfo) {
                ClientBulkWriteCommand.OpsAndNsInfo opsAndNsInfo = (ClientBulkWriteCommand.OpsAndNsInfo) sequences;
                try (ByteBufferBsonOutput.Branch bsonOutputBranch2 = bsonOutput.branch();
                     ByteBufferBsonOutput.Branch bsonOutputBranch1 = bsonOutput.branch()) {
                    ClientBulkWriteCommand.OpsAndNsInfo.EncodeResult opsAndNsInfoEncodeResult = writeOpMsgSectionWithPayloadType1(
                            bsonOutputBranch1, "ops", () ->
                                    writeOpMsgSectionWithPayloadType1(bsonOutputBranch2, "nsInfo", () ->
                                            writeOpsAndNsInfo(
                                                    opsAndNsInfo, commandDocumentSizeInBytes, bsonOutputBranch1,
                                                    bsonOutputBranch2, getSettings(), validateDocumentSizeLimits)
                                    )
                    );
                    opsAndNsInfoRequireResponse = opsAndNsInfoEncodeResult.isServerResponseRequired();
                    Long txnNumber = opsAndNsInfoEncodeResult.getTxnNumber();
                    if (txnNumber != null) {
                        extraElements.add(new BsonElement(TXN_NUMBER_KEY, new BsonInt64(txnNumber)));
                    }
                    appendElementsToDocument(bsonOutput, commandStartPosition, extraElements);
                }
            } else if (sequences instanceof EmptyOpMsgSequences) {
                appendElementsToDocument(bsonOutput, commandStartPosition, extraElements);
            } else {
                fail(sequences.toString());
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
            writeDocument(command, bsonOutput, commandFieldNameValidator, true);
            appendElementsToDocument(bsonOutput, commandStartPosition, elements);
        }
        return new EncodingMetadata(commandStartPosition);
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

    private ArrayList<BsonElement> getExtraElements(final OperationContext operationContext) {
        SessionContext sessionContext = operationContext.getSessionContext();
        TimeoutContext timeoutContext = operationContext.getTimeoutContext();

        ArrayList<BsonElement> extraElements = new ArrayList<>();
        if (!getSettings().isCryptd()) {
           timeoutContext.runMaxTimeMS(maxTimeMS ->
                   extraElements.add(new BsonElement("maxTimeMS", new BsonInt64(maxTimeMS)))
           );
        }
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
            extraElements.add(new BsonElement(TXN_NUMBER_KEY, new BsonInt64(sessionContext.getTransactionNumber())));
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

    private <R> R writeOpMsgSectionWithPayloadType1(
            final ByteBufferBsonOutput bsonOutput,
            final String sequenceId,
            final Supplier<R> writeDocumentsAction) {
        // payload type
        bsonOutput.writeByte(1);
        int sequenceStart = bsonOutput.getPosition();
        // size to be patched back later
        bsonOutput.writeInt32(0);
        bsonOutput.writeCString(sequenceId);
        R result = writeDocumentsAction.get();
        backpatchLength(sequenceStart, bsonOutput);
        return result;
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
