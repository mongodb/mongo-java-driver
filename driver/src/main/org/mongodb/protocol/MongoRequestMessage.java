/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.protocol;

import org.bson.BSONBinaryWriter;
import org.mongodb.io.ChannelAwareOutputBuffer;
import org.mongodb.serialization.Serializer;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract base class for all MongoDB Wire Protocol request messages.
 */
public abstract class MongoRequestMessage {
    // TODO: is rollover a problem
    static final AtomicInteger REQUEST_ID = new AtomicInteger(1);

    private final String collectionName;
    private final int id;
    private final OpCode opCode;

    public MongoRequestMessage(final String collectionName, final OpCode opCode) {
        this.collectionName = collectionName;
        id = REQUEST_ID.getAndIncrement();
        this.opCode = opCode;
    }

    public MongoRequestMessage(final OpCode opCode) {
        this(null, opCode);
    }

    protected void writeMessagePrologue(final ChannelAwareOutputBuffer buffer) {
        buffer.writeInt(0); // length: will set this later
        buffer.writeInt(id);
        buffer.writeInt(0); // response to
        buffer.writeInt(opCode.getValue());
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

    public void serialize(final ChannelAwareOutputBuffer buffer) {
        int messageStartPosition = buffer.getPosition();
        writeMessagePrologue(buffer);
        serializeMessageBody(buffer);
        backpatchMessageLength(messageStartPosition, buffer);
    }

    protected abstract void serializeMessageBody(final ChannelAwareOutputBuffer buffer);

    protected <T> void addDocument(final T obj, final Serializer<T> serializer, final ChannelAwareOutputBuffer buffer) {
        final BSONBinaryWriter writer = new BSONBinaryWriter(buffer);

        try {
            serializer.serialize(writer, obj);
        } finally {
            writer.close();
        }

        // TODO: Figure out how to deal with exceeding max BSON object size
    }

    protected void backpatchMessageLength(final int startPosition, final ChannelAwareOutputBuffer buffer) {
        final int messageLength = buffer.getPosition() - startPosition;
        buffer.backpatchSize(messageLength);
    }

    protected String getCollectionName() {
        return collectionName;
    }

    enum OpCode {
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
