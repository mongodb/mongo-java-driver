/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.mongodb.protocol;

import org.bson.BSONBinaryWriter;
import org.bson.BinaryWriterSettings;
import org.bson.BsonWriterSettings;
import org.bson.io.OutputBuffer;
import org.bson.types.Document;
import org.mongodb.ReadPreference;
import org.mongodb.serialization.Serializer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract base class for all MongoDB Wire Protocol request messages.
 */
public class MongoRequestMessage {

    // TODO: probably only needs to be unique per connection, not VM
    // TODO: is rollover a problem
    static final AtomicInteger REQUEST_ID = new AtomicInteger(1);

    protected final String collectionName;
    protected final OutputBuffer buffer;
    private final int id;
    private final OpCode opCode;
    protected final Document query;  // TODO: does this field need to exist?
    private volatile int numDocuments; // only one thread will modify this field, so volatile is sufficient synchronization
    private final int messageStartPosition;

    MongoRequestMessage(final OpCode opCode, final OutputBuffer buffer) {
        this(null, opCode, buffer);
    }

    MongoRequestMessage(final String collectionName, final OpCode opCode, final OutputBuffer buffer) {
        this(collectionName, opCode, null, -1, null, buffer);
    }

    MongoRequestMessage(final String collectionName, final OpCode opCode, final Document query, final OutputBuffer buffer) {
        this(collectionName, opCode, query, 0, null, buffer);
    }

    MongoRequestMessage(final String collectionName, final Document query, final int options, final ReadPreference readPref,
                        final OutputBuffer buffer) {
        this(collectionName, OpCode.OP_QUERY, query, options, readPref, buffer);
    }

    MongoRequestMessage(final String collectionName, final OpCode opCode, final Document query,
                        final int options, final ReadPreference readPreference, final OutputBuffer buffer) {
        this.collectionName = collectionName;

        this.buffer = buffer;
        messageStartPosition = buffer.getPosition();

        id = REQUEST_ID.getAndIncrement();
        this.opCode = opCode;
        this.query = query;

        writeMessagePrologue(opCode);
    }

    private void writeMessagePrologue(final OpCode opCode) {
        buffer.writeInt(0); // length: will set this later
        buffer.writeInt(id);
        buffer.writeInt(0); // response to
        buffer.writeInt(opCode.getValue());
    }

    void pipe(final OutputStream out) throws IOException {
        buffer.pipe(out);
    }

    int size() {
        return buffer.size();
    }

    void doneWithMessage() {
        try {
            buffer.close();
        } catch (IOException e) {
            // TODO: Maybe not throw IOException from close()
        }
    }

    int getId() {
        return id;
    }

    OpCode getOpCode() {
        return opCode;
    }

    Map<String, Object> getQuery() {
        return query;
    }

    String getNamespace() {
        return collectionName != null ? collectionName : null;
    }

    int getNumDocuments() {
        return numDocuments;
    }

    public <T> void addDocument(final T obj, final Serializer<T> serializer) {
        // TODO fix this constructor call to remove hard coding
        final BSONBinaryWriter writer = new BSONBinaryWriter(new BsonWriterSettings(100),
                new BinaryWriterSettings(1024 * 1024 * 16), buffer);

        try {
            // TODO: deal with serialization options
            serializer.serialize(writer, obj, null);
        } finally {
            writer.close();
        }

        // TODO: Figure out how to deal with exceeding max BSON object size
        numDocuments++;
        backpatchMessageLength();
    }

    protected void backpatchMessageLength() {
        final int messageLength = buffer.getPosition() - messageStartPosition;
        buffer.backpatchSize(messageLength);
    }

    public OutputBuffer getBuffer() {
        return buffer;
    }

    public void done() throws IOException {
        buffer.close();
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
