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
import org.bson.io.OutputBuffer;
import org.bson.io.async.AsyncCompletionHandler;
import org.bson.io.async.AsyncWritableByteChannel;
import org.mongodb.ReadPreference;
import org.mongodb.serialization.Serializer;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract base class for all MongoDB Wire Protocol request messages.
 */
public class MongoRequestMessage {
    // TODO: is rollover a problem
    static final AtomicInteger REQUEST_ID = new AtomicInteger(1);

    private final String collectionName;
    private volatile OutputBuffer buffer;
    private final int id;
    private final OpCode opCode;
    private volatile int numDocuments; // only one thread will modify this field, so volatile is sufficient synchronization
    private final int messageStartPosition;

    MongoRequestMessage(final OpCode opCode, final OutputBuffer buffer) {
        this(null, opCode, buffer);
    }

    MongoRequestMessage(final String collectionName, final OpCode opCode, final OutputBuffer buffer) {
        this(collectionName, opCode, -1, null, buffer);
    }

    MongoRequestMessage(final String collectionName, final int options, final ReadPreference readPref,
                        final OutputBuffer buffer) {
        this(collectionName, OpCode.OP_QUERY, options, readPref, buffer);
    }

    MongoRequestMessage(final String collectionName, final OpCode opCode,
                        final int options, final ReadPreference readPreference, final OutputBuffer buffer) {
        this.collectionName = collectionName;

        this.buffer = buffer;
        messageStartPosition = buffer.getPosition();

        id = REQUEST_ID.getAndIncrement();
        this.opCode = opCode;

        writeMessagePrologue(opCode);
    }

    //CHECKSTYLE:OFF
    private void writeMessagePrologue(final OpCode opCode) {
        getBuffer().writeInt(0); // length: will set this later
        getBuffer().writeInt(id);
        getBuffer().writeInt(0); // response to
        getBuffer().writeInt(opCode.getValue());
    }
    //CHECKSTYLE:ON

    public void pipe(final SocketChannel out) throws IOException {
        getBuffer().pipe(out);
    }

    public void pipe(final AsyncWritableByteChannel channel, final AsyncCompletionHandler handler)
            throws ExecutionException, InterruptedException {
        getBuffer().pipe(channel, handler);
    }

    public int size() {
        return getBuffer().size();
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

    public int getNumDocuments() {
        return numDocuments;
    }

    protected  <T> void addDocument(final T obj, final Serializer<T> serializer) {
        final BSONBinaryWriter writer = new BSONBinaryWriter(getBuffer());

        try {
            serializer.serialize(writer, obj);
        } finally {
            writer.close();
        }

        // TODO: Figure out how to deal with exceeding max BSON object size
        numDocuments++;
        backpatchMessageLength();
    }

    protected void backpatchMessageLength() {
        final int messageLength = getBuffer().getPosition() - messageStartPosition;
        getBuffer().backpatchSize(messageLength);
    }

    public void close() {
        if (getBuffer() == null) {
            throw new IllegalStateException("Buffer is already closed");
        }
        getBuffer().close();
        buffer = null;
    }

    protected OutputBuffer getBuffer() {
        return buffer;
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
