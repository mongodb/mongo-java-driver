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

import com.mongodb.lang.Nullable;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.io.BsonOutput;

import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.BsonWriterHelper.backpatchLength;
import static com.mongodb.internal.connection.BsonWriterHelper.createBsonBinaryWriter;
import static com.mongodb.internal.connection.BsonWriterHelper.encodeUsingRegistry;

/**
 * Abstract base class for all MongoDB Wire Protocol request messages.
 */
abstract class RequestMessage {

    static final AtomicInteger REQUEST_ID = new AtomicInteger(1);

    static final int MESSAGE_PROLOGUE_LENGTH = 16;

    private final String collectionName;
    private final MessageSettings settings;
    private final int id;
    private final OpCode opCode;
    private EncodingMetadata encodingMetadata;

    static class EncodingMetadata {
        private final int firstDocumentPosition;

        EncodingMetadata(final int firstDocumentPosition) {
            this.firstDocumentPosition = firstDocumentPosition;
        }

        public int getFirstDocumentPosition() {
            return firstDocumentPosition;
        }
    }
    /**
     * Gets the next available unique message identifier.
     *
     * @return the message identifier
     */
    public static int getCurrentGlobalId() {
        return REQUEST_ID.get();
    }

    RequestMessage(final OpCode opCode, final int requestId, final MessageSettings settings) {
        this(null, opCode, requestId, settings);
    }


    RequestMessage(final String collectionName, final OpCode opCode, final MessageSettings settings) {
        this(collectionName, opCode, REQUEST_ID.getAndIncrement(), settings);
    }

    private RequestMessage(@Nullable final String collectionName, final OpCode opCode, final int requestId,
                           final MessageSettings settings) {
        this.collectionName = collectionName;
        this.settings = settings;
        id = requestId;
        this.opCode = opCode;
    }

    /**
     * Gets the message id.
     *
     * @return the message id
     */
    public int getId() {
        return id;
    }

    /**
     * Gets the op code of the message.
     *
     * @return the op code
     */
    public OpCode getOpCode() {
        return opCode;
    }

    /**
     * Gets the message settings.
     *
     * @return the message settings
     */
    public MessageSettings getSettings() {
        return settings;
    }

    /**
     * Encoded the message to the given output.
     *
     * @param bsonOutput the output
     * @param operationContext the session context
     */
    public void encode(final ByteBufferBsonOutput bsonOutput, final OperationContext operationContext) {
        notNull("operationContext", operationContext);
        int messageStartPosition = bsonOutput.getPosition();
        writeMessagePrologue(bsonOutput);
        EncodingMetadata encodingMetadata = encodeMessageBodyWithMetadata(bsonOutput, operationContext);
        backpatchLength(messageStartPosition, bsonOutput);
        this.encodingMetadata = encodingMetadata;
    }

    /**
     * Gets the encoding metadata from the last attempt to encode this message.
     *
     * @return Get metadata from the last attempt to encode this message. Returns null if there has not yet been an attempt.
     */
    public EncodingMetadata getEncodingMetadata() {
        return encodingMetadata;
    }

    /**
     * Writes the message prologue to the given output.
     *
     * @param bsonOutput the output
     */
    protected void writeMessagePrologue(final BsonOutput bsonOutput) {
        bsonOutput.writeInt32(0); // length: will set this later
        bsonOutput.writeInt32(id);
        bsonOutput.writeInt32(0); // response to
        bsonOutput.writeInt32(opCode.getValue());
    }

    /**
     * Encode the message body to the given output.
     *
     * @param bsonOutput the output
     * @param operationContext the session context
     * @return the encoding metadata
     */
    protected abstract EncodingMetadata encodeMessageBodyWithMetadata(ByteBufferBsonOutput bsonOutput, OperationContext operationContext);

    protected int writeDocument(final BsonDocument document, final BsonOutput bsonOutput,
                               final FieldNameValidator validator, final boolean validateMaxBsonObjectSize) {
        BsonBinaryWriter writer = createBsonBinaryWriter(bsonOutput, validator, validateMaxBsonObjectSize ? getSettings() : null);
        int documentStart = bsonOutput.getPosition();
        encodeUsingRegistry(writer, document);
        return bsonOutput.getPosition() - documentStart;
    }

    /**
     * Gets the collection name, which may be null for some message types
     *
     * @return the collection name
     */
    protected String getCollectionName() {
        return collectionName;
    }
}
