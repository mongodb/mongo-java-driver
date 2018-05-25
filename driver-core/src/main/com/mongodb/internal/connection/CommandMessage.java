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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.connection.ByteBufferBsonOutput;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ServerVersion;
import com.mongodb.connection.SplittablePayload;
import com.mongodb.internal.validator.MappedFieldNameValidator;
import com.mongodb.session.SessionContext;
import org.bson.BsonArray;
import org.bson.BsonBinaryWriter;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonWriter;
import org.bson.FieldNameValidator;
import org.bson.codecs.EncoderContext;
import org.bson.io.BsonOutput;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.ReadPreference.primaryPreferred;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static com.mongodb.connection.ServerType.SHARD_ROUTER;
import static com.mongodb.internal.connection.BsonWriterHelper.writePayload;
import static com.mongodb.internal.connection.ReadConcernHelper.getReadConcernDocument;

/**
 * A command message that uses OP_MSG or OP_QUERY to send the command.
 */
public final class CommandMessage extends RequestMessage {
    private final MongoNamespace namespace;
    private final BsonDocument command;
    private final FieldNameValidator commandFieldNameValidator;
    private final ReadPreference readPreference;
    private final SplittablePayload payload;
    private final FieldNameValidator payloadFieldNameValidator;
    private final boolean responseExpected;
    private final ClusterConnectionMode clusterConnectionMode;

    CommandMessage(final MongoNamespace namespace, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                   final ReadPreference readPreference, final MessageSettings settings) {
        this(namespace, command, commandFieldNameValidator, readPreference, settings, true, null, null,
                MULTIPLE);
    }

    CommandMessage(final MongoNamespace namespace, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                   final ReadPreference readPreference, final MessageSettings settings, final boolean responseExpected,
                   final SplittablePayload payload, final FieldNameValidator payloadFieldNameValidator,
                   final ClusterConnectionMode clusterConnectionMode) {
        super(namespace.getFullName(), getOpCode(settings), settings);
        this.namespace = namespace;
        this.command = command;
        this.commandFieldNameValidator = commandFieldNameValidator;
        this.readPreference = readPreference;
        this.responseExpected = responseExpected;
        this.payload = payload;
        this.payloadFieldNameValidator = payloadFieldNameValidator;
        this.clusterConnectionMode = clusterConnectionMode;
    }

    BsonDocument getCommandDocument(final ByteBufferBsonOutput bsonOutput) {
        ByteBufBsonDocument byteBufBsonDocument = ByteBufBsonDocument.createOne(bsonOutput,
                getEncodingMetadata().getFirstDocumentPosition());
        BsonDocument commandBsonDocument;

        if (useOpMsg() && containsPayload()) {
            commandBsonDocument = byteBufBsonDocument.toBsonDocument();

            int payloadStartPosition = getEncodingMetadata().getFirstDocumentPosition()
                    + byteBufBsonDocument.getSizeInBytes()
                    + 1 // payload type
                    + 4 // payload size
                    + payload.getPayloadName().getBytes(Charset.forName("UTF-8")).length + 1;  // null-terminated UTF-8 payload name
            commandBsonDocument.append(payload.getPayloadName(),
                    new BsonArray(ByteBufBsonDocument.createList(bsonOutput, payloadStartPosition)));
        } else {
            commandBsonDocument = byteBufBsonDocument;
        }

        if (commandBsonDocument.containsKey("$query")) {
            commandBsonDocument = commandBsonDocument.getDocument("$query");
        }
        return commandBsonDocument;
    }

    boolean containsPayload() {
        return payload != null;
    }

    boolean isResponseExpected() {
        isTrue("The message must be encoded before determining if a response is expected", getEncodingMetadata() != null);
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

            addDocument(getCommandToEncode(), bsonOutput, commandFieldNameValidator, getExtraElements(sessionContext));

            if (payload != null) {
                bsonOutput.writeByte(1);          // payload type
                int payloadBsonOutputStartPosition = bsonOutput.getPosition();
                bsonOutput.writeInt32(0);         // size
                bsonOutput.writeCString(payload.getPayloadName());
                writePayload(new BsonBinaryWriter(bsonOutput, payloadFieldNameValidator), bsonOutput, getSettings(),
                        messageStartPosition, payload);

                int payloadBsonOutputLength = bsonOutput.getPosition() - payloadBsonOutputStartPosition;
                bsonOutput.writeInt32(payloadBsonOutputStartPosition, payloadBsonOutputLength);
            }

            // Write the flag bits
            bsonOutput.writeInt32(flagPosition, getOpMsgFlagBits());
        } else {
            bsonOutput.writeInt32(getOpQueryFlagBits());
            bsonOutput.writeCString(namespace.getFullName());
            bsonOutput.writeInt32(0);
            bsonOutput.writeInt32(-1);

            commandStartPosition = bsonOutput.getPosition();

            if (payload == null) {
                addDocument(getCommandToEncode(), bsonOutput, commandFieldNameValidator, null);
            } else {
                addDocumentWithPayload(bsonOutput, messageStartPosition);
            }
        }
        return new EncodingMetadata(commandStartPosition);
    }

    private FieldNameValidator getPayloadArrayFieldNameValidator() {
        Map<String, FieldNameValidator> rootMap = new HashMap<String, FieldNameValidator>();
        rootMap.put(payload.getPayloadName(), payloadFieldNameValidator);
        return new MappedFieldNameValidator(commandFieldNameValidator, rootMap);
    }

    private void addDocumentWithPayload(final BsonOutput bsonOutput, final int messageStartPosition) {
        BsonBinaryWriter bsonBinaryWriter = new BsonBinaryWriter(bsonOutput, getPayloadArrayFieldNameValidator());
        BsonWriter bsonWriter = new SplittablePayloadBsonWriter(bsonBinaryWriter, bsonOutput, messageStartPosition, getSettings(), payload);
        BsonDocument commandToEncode = getCommandToEncode();
        getCodec(commandToEncode).encode(bsonWriter, commandToEncode, EncoderContext.builder().build());
    }

    private int getOpMsgFlagBits() {
        return getOpMsgResponseExpectedFlagBit();
    }

    private int getOpMsgResponseExpectedFlagBit() {
        if (requireOpMsgResponse()) {
            return 0;
        } else {
            return 1 << 1;
        }
    }

    private boolean requireOpMsgResponse() {
        if (responseExpected) {
            return true;
        } else {
            return payload != null && payload.hasAnotherSplit();
        }
    }

    private int getOpQueryFlagBits() {
        return getOpQuerySlaveOkFlagBit();
    }

    private int getOpQuerySlaveOkFlagBit() {
        if (isSlaveOk()) {
            return 1 << 2;
        } else {
            return 0;
        }
    }

    private boolean isSlaveOk() {
        return (readPreference != null && readPreference.isSlaveOk()) || isDirectConnectionToNonShardRouter();
    }

    private boolean isDirectConnectionToNonShardRouter() {
        return clusterConnectionMode == SINGLE && getSettings().getServerType() != SHARD_ROUTER;
    }

    private boolean useOpMsg() {
        return getOpCode().equals(OpCode.OP_MSG);
    }

    private BsonDocument getCommandToEncode() {
        BsonDocument commandToEncode = command;
        if (!useOpMsg() && readPreference != null && !readPreference.equals(primary())) {
            commandToEncode = new BsonDocument("$query", command).append("$readPreference", readPreference.toDocument());
        }
        return commandToEncode;
    }

    private List<BsonElement> getExtraElements(final SessionContext sessionContext) {
        List<BsonElement> extraElements = new ArrayList<BsonElement>();
        extraElements.add(new BsonElement("$db", new BsonString(new MongoNamespace(getCollectionName()).getDatabaseName())));
        if (sessionContext.getClusterTime() != null) {
            extraElements.add(new BsonElement("$clusterTime", sessionContext.getClusterTime()));
        }
        if (sessionContext.hasSession() && responseExpected) {
            extraElements.add(new BsonElement("lsid", sessionContext.getSessionId()));
        }
        if (sessionContext.hasActiveTransaction()) {
            extraElements.add(new BsonElement("txnNumber", new BsonInt64(sessionContext.getTransactionNumber())));
            boolean firstMessage = sessionContext.notifyMessageSent();
            if (firstMessage) {
                extraElements.add(new BsonElement("startTransaction", BsonBoolean.TRUE));
                addReadConcernDocument(extraElements, sessionContext);
            }
            extraElements.add(new BsonElement("autocommit", BsonBoolean.FALSE));
        }
        if (readPreference != null) {
            if (!readPreference.equals(primary())) {
                extraElements.add(new BsonElement("$readPreference", readPreference.toDocument()));
            } else if (isDirectConnectionToNonShardRouter()) {
                extraElements.add(new BsonElement("$readPreference", primaryPreferred().toDocument()));
            }
        }
        return extraElements;
    }

    private void addReadConcernDocument(final List<BsonElement> extraElements, final SessionContext sessionContext) {
        BsonDocument readConcernDocument = getReadConcernDocument(sessionContext);
        if (!readConcernDocument.isEmpty()) {
            extraElements.add(new BsonElement("readConcern", readConcernDocument));
        }
    }

    private static OpCode getOpCode(final MessageSettings settings) {
        return isServerVersionAtLeastThreeDotSix(settings) ? OpCode.OP_MSG : OpCode.OP_QUERY;
    }

    private static boolean isServerVersionAtLeastThreeDotSix(final MessageSettings settings) {
        return settings.getServerVersion().compareTo(new ServerVersion(3, 6)) >= 0;
    }

}
