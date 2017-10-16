/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.connection;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonType;
import org.bson.ByteBuf;

import java.io.ByteArrayOutputStream;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

class ByteBufPayloadBsonDocument extends AbstractByteBufBsonDocument implements Cloneable, Serializable {
    private static final long serialVersionUID = 3L;
    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

    private final ByteBufBsonDocument commandDocument;
    private final String payloadName;
    private final List<ByteBufBsonDocument> payload;
    private BsonDocument unwrapped;


    static AbstractByteBufBsonDocument create(final ByteBufferBsonOutput bsonOutput, final int startPosition) {
        List<ByteBuf> duplicateByteBuffers = bsonOutput.getByteBuffers();
        CompositeByteBuf outputByteBuf = new CompositeByteBuf(duplicateByteBuffers);
        outputByteBuf.position(startPosition);

        ByteBufBsonDocument commandDocument = createByteBufBsonDocument(outputByteBuf);
        if (!outputByteBuf.hasRemaining()) {
            return commandDocument;
        }

        String payloadName = validatePayloadAndGetName(outputByteBuf);
        List<ByteBufBsonDocument> payload = createPayload(outputByteBuf);

        for (ByteBuf byteBuffer : duplicateByteBuffers) {
            byteBuffer.release();
        }
        return new ByteBufPayloadBsonDocument(commandDocument, payloadName, payload);
    }

    <T> T findInDocument(final Finder<T> finder) {
        BsonDocumentReader bsonReader = new BsonDocumentReader(toBsonDocument());
        try {
            bsonReader.readStartDocument();
            while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                T found = finder.find(bsonReader);
                if (found != null) {
                    return found;
                }
            }
            bsonReader.readEndDocument();
        } finally {
            bsonReader.close();
        }
        return finder.notFound();
    }

    @Override
    public BsonDocument clone() {
        return toBsonDocument();
    }

    BsonDocument toBsonDocument() {
        if (unwrapped == null) {
            unwrapped =  commandDocument.toBsonDocument().append(payloadName, new BsonArray(payload));
        }
        return unwrapped;
    }

    ByteBufPayloadBsonDocument(final ByteBufBsonDocument commandDocument, final String payloadName,
                               final List<ByteBufBsonDocument> payload) {
        this.commandDocument = commandDocument;
        this.payloadName = payloadName;
        this.payload = payload;
    }

    private static List<ByteBufBsonDocument> createPayload(final CompositeByteBuf outputByteBuf) {
        List<ByteBufBsonDocument> payload = new ArrayList<ByteBufBsonDocument>();
        while (outputByteBuf.hasRemaining()) {
            payload.add(createByteBufBsonDocument(outputByteBuf));
        }
        return payload;
    }

    private static ByteBufBsonDocument createByteBufBsonDocument(final CompositeByteBuf outputByteBuf) {
        int startPosition = outputByteBuf.position();
        int documentSizeInBytes = outputByteBuf.getInt();
        ByteBuf slice = outputByteBuf.duplicate();
        slice.position(startPosition);
        slice.limit(startPosition + documentSizeInBytes);
        outputByteBuf.position(startPosition + documentSizeInBytes);
        return new ByteBufBsonDocument(slice);
    }

    private static String validatePayloadAndGetName(final CompositeByteBuf outputByteBuf) {
        if (outputByteBuf.get() != 1) {
            throw new IllegalArgumentException("Invalid payload type");
        }
        int sliceSize = outputByteBuf.remaining();
        int payloadSize = outputByteBuf.getInt();
        if (sliceSize != payloadSize) {
            throw new IllegalArgumentException("Invalid payload size");
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte nextByte = outputByteBuf.get();
        while (nextByte != 0) {
            outputStream.write(nextByte);
            nextByte = outputByteBuf.get();
        }
        return new String(outputStream.toByteArray(), UTF8_CHARSET);
    }

    // see https://docs.oracle.com/javase/6/docs/platform/serialization/spec/input.html
    private void readObject(final ObjectInputStream stream) throws InvalidObjectException {
        throw new InvalidObjectException("Proxy required");
    }
}
