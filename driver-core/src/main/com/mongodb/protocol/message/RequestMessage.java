/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.protocol.message;

import org.bson.BsonBinaryWriter;
import org.bson.BsonBinaryWriterSettings;
import org.bson.BsonWriterSettings;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.io.OutputBuffer;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract base class for all MongoDB Wire Protocol request messages.
 */
public abstract class RequestMessage {

    static final AtomicInteger REQUEST_ID = new AtomicInteger(1);

    // Allow an extra 16K to the maximum allowed size of a query or command document, so that, for example,
    // a 16M document can be upserted via findAndModify
    private static final int QUERY_DOCUMENT_HEADROOM = 16 * 1024;

    private final String collectionName;
    private final MessageSettings settings;
    private final int id;
    private final OpCode opCode;

    protected BsonDocumentCodec getBsonDocumentCodec() {
        return new BsonDocumentCodec();
    }

    public RequestMessage(final String collectionName, final OpCode opCode, final MessageSettings settings) {
        this.collectionName = collectionName;
        this.settings = settings;
        id = REQUEST_ID.getAndIncrement();
        this.opCode = opCode;
    }

    public RequestMessage(final OpCode opCode, final MessageSettings settings) {
        this(null, opCode, settings);
    }

    protected void writeMessagePrologue(final OutputBuffer buffer) {
        buffer.writeInt(0); // length: will set this later
        buffer.writeInt(id);
        buffer.writeInt(0); // response to
        buffer.writeInt(opCode.getValue());
    }

    public static int getCurrentGlobalId() {
        return REQUEST_ID.get();
    }

    public int getId() {
        return id;
    }

    public OpCode getOpCode() {
        return opCode;
    }

    public String getNamespace() {
        return getCollectionName() != null ? getCollectionName() : null;
    }


    public MessageSettings getSettings() {
        return settings;
    }

    public RequestMessage encode(final OutputBuffer buffer) {
        int messageStartPosition = buffer.getPosition();
        writeMessagePrologue(buffer);
        RequestMessage nextMessage = encodeMessageBody(buffer, messageStartPosition);
        backpatchMessageLength(messageStartPosition, buffer);
        return nextMessage;
    }

    protected abstract RequestMessage encodeMessageBody(final OutputBuffer buffer, final int messageStartPosition);

    protected <T> void addDocument(final T obj, final Encoder<T> encoder, final OutputBuffer buffer, final FieldNameValidator validator) {
        addDocument(obj, encoder, EncoderContext.builder().build(), buffer, validator,
                    settings.getMaxDocumentSize() + QUERY_DOCUMENT_HEADROOM);
    }

    protected <T> void addCollectibleDocument(final T obj, final Encoder<T> encoder, final OutputBuffer buffer,
                                              final FieldNameValidator validator) {
        addDocument(obj, encoder, EncoderContext.builder().isEncodingCollectibleDocument(true).build(), buffer, validator,
                    settings.getMaxDocumentSize());
    }

    private <T> void addDocument(final T obj, final Encoder<T> encoder, final EncoderContext encoderContext, final OutputBuffer buffer,
                                 final FieldNameValidator validator, final int maxDocumentSize) {
        BsonBinaryWriter writer = new BsonBinaryWriter(new BsonWriterSettings(),
                                                       new BsonBinaryWriterSettings(maxDocumentSize), buffer, validator);
        try {
            encoder.encode(writer, obj, encoderContext);
        } finally {
            writer.close();
        }
    }

    protected void backpatchMessageLength(final int startPosition, final OutputBuffer buffer) {
        int messageLength = buffer.getPosition() - startPosition;
        buffer.backpatchSize(messageLength);
    }

    protected String getCollectionName() {
        return collectionName;
    }

    enum OpCode {
        OP_REPLY(1),
        OP_MSG(1000),
        OP_UPDATE(2001),
        OP_INSERT(2002),
        OP_QUERY(2004),
        OP_GETMORE(2005),
        OP_DELETE(2006),
        OP_KILL_CURSORS(2007);

        OpCode(final int value) {
            this.value = value;
        }

        private final int value;

        public int getValue() {
            return value;
        }
    }
}
