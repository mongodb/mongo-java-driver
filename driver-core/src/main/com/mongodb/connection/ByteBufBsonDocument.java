/*
 * Copyright 2015 MongoDB, Inc.
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
 *
 */

package com.mongodb.connection;

import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.ByteBuf;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.ByteBufferBsonInput;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.StringWriter;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.bson.codecs.BsonValueCodecProvider.getClassForBsonType;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

class ByteBufBsonDocument extends BsonDocument implements Cloneable {
    private static final long serialVersionUID = 1L;

    private static final CodecRegistry REGISTRY = fromProviders(new BsonValueCodecProvider());

    private final transient ByteBuf byteBuf;

    static List<ByteBufBsonDocument> create(final ResponseBuffers responseBuffers) {
        int numDocuments = responseBuffers.getReplyHeader().getNumberReturned();
        ByteBuf documentsBuffer = responseBuffers.getBodyByteBuffer();
        documentsBuffer.order(ByteOrder.LITTLE_ENDIAN);
        List<ByteBufBsonDocument> documents = new ArrayList<ByteBufBsonDocument>(numDocuments);
        while (documents.size() < numDocuments) {
            int documentSizeInBytes = documentsBuffer.getInt();
            documentsBuffer.position(documentsBuffer.position() - 4);
            ByteBuf documentBuffer = documentsBuffer.duplicate();
            documentBuffer.limit(documentBuffer.position() + documentSizeInBytes);
            documents.add(new ByteBufBsonDocument(documentBuffer));
            documentsBuffer.position(documentsBuffer.position() + documentSizeInBytes);
        }

        return documents;
    }

    static ByteBufBsonDocument createOne(final ByteBufferBsonOutput bsonOutput, final int startPosition) {
        return create(bsonOutput, startPosition).get(0);
    }

    static List<ByteBufBsonDocument> create(final ByteBufferBsonOutput bsonOutput, final int startPosition) {
        List<ByteBuf> duplicateByteBuffers = bsonOutput.getByteBuffers();
        CompositeByteBuf outputByteBuf = new CompositeByteBuf(duplicateByteBuffers);
        outputByteBuf.position(startPosition);
        List<ByteBufBsonDocument> documents = new ArrayList<ByteBufBsonDocument>();
        int curDocumentStartPosition = startPosition;
        while (outputByteBuf.hasRemaining()) {
            int documentSizeInBytes = outputByteBuf.getInt();
            ByteBuf slice = outputByteBuf.duplicate();
            slice.position(curDocumentStartPosition);
            slice.limit(curDocumentStartPosition + documentSizeInBytes);
            documents.add(new ByteBufBsonDocument(slice));
            curDocumentStartPosition += documentSizeInBytes;
            outputByteBuf.position(outputByteBuf.position() + documentSizeInBytes - 4);
        }
        for (ByteBuf byteBuffer : duplicateByteBuffers) {
            byteBuffer.release();
        }
        return documents;
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("RawBsonDocument instances are immutable");
    }

    @Override
    public BsonValue put(final String key, final BsonValue value) {
        throw new UnsupportedOperationException("RawBsonDocument instances are immutable");
    }

    @Override
    public BsonDocument append(final String key, final BsonValue value) {
        throw new UnsupportedOperationException("RawBsonDocument instances are immutable");
    }

    @Override
    public void putAll(final Map<? extends String, ? extends BsonValue> m) {
        throw new UnsupportedOperationException("RawBsonDocument instances are immutable");
    }

    @Override
    public BsonValue remove(final Object key) {
        throw new UnsupportedOperationException("RawBsonDocument instances are immutable");
    }

    @Override
    public boolean isEmpty() {
        return findInDocument(new Finder<Boolean>() {
            @Override
            public Boolean find(final BsonReader bsonReader) {
                return false;
            }

            @Override
            public Boolean notFound() {
                return true;
            }
        });
    }

    @Override
    public int size() {
        return findInDocument(new Finder<Integer>() {
            private int size;

            @Override
            public Integer find(final BsonReader bsonReader) {
                size++;
                bsonReader.readName();
                bsonReader.skipValue();
                return null;
            }

            @Override
            public Integer notFound() {
                return size;
            }
        });
    }

    @Override
    public Set<Entry<String, BsonValue>> entrySet() {
        return toBsonDocument().entrySet();
    }

    @Override
    public Collection<BsonValue> values() {
        return toBsonDocument().values();
    }

    @Override
    public Set<String> keySet() {
        return toBsonDocument().keySet();
    }

    @Override
    public boolean containsKey(final Object key) {
        if (key == null) {
            throw new IllegalArgumentException("key can not be null");
        }

        return findInDocument(new Finder<Boolean>() {
            @Override
            public Boolean find(final BsonReader bsonReader) {
                if (bsonReader.readName().equals(key)) {
                    return true;
                }
                bsonReader.skipValue();
                return null;
            }

            @Override
            public Boolean notFound() {
                return false;
            }
        });
    }

    @Override
    public boolean containsValue(final Object value) {
        return findInDocument(new Finder<Boolean>() {
            @Override
            public Boolean find(final BsonReader bsonReader) {
                bsonReader.skipName();
                if (deserializeBsonValue(bsonReader).equals(value)) {
                    return true;
                }
                return null;
            }

            @Override
            public Boolean notFound() {
                return false;
            }
        });
    }

    @Override
    public BsonValue get(final Object key) {
        if (key == null) {
            throw new IllegalArgumentException("key can not be null");
        }

        return findInDocument(new Finder<BsonValue>() {
            @Override
            public BsonValue find(final BsonReader bsonReader) {
                if (bsonReader.readName().equals(key)) {
                    return deserializeBsonValue(bsonReader);
                }
                bsonReader.skipValue();
                return null;
            }

            @Override
            public BsonValue notFound() {
                return null;
            }
        });
    }

    @Override
    @SuppressWarnings("deprecation")
    public String toJson() {
        return toJson(new JsonWriterSettings());
    }

    @Override
    public String toJson(final JsonWriterSettings settings) {
        StringWriter stringWriter = new StringWriter();
        JsonWriter jsonWriter = new JsonWriter(stringWriter, settings);
        ByteBuf duplicate = byteBuf.duplicate();
        BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(duplicate));
        try {
            jsonWriter.pipe(reader);
            return stringWriter.toString();
        } finally {
            duplicate.release();
            reader.close();
        }
    }

    /**
     * Gets the first key in this document, or null if the document is empty.
     *
     * @return the first key in this document, or null if the document is empty
     */
    public String getFirstKey() {
        return findInDocument(new Finder<String>() {
            @Override
            public String find(final BsonReader bsonReader) {
                return bsonReader.readName();
            }

            @Override
            public String notFound() {
                return null;
            }
        });
    }

    private interface Finder<T> {
        T find(BsonReader bsonReader);
        T notFound();
    }

    private <T> T findInDocument(final Finder<T> finder) {
        ByteBuf duplicateByteBuf = byteBuf.duplicate();
        BsonBinaryReader bsonReader = new BsonBinaryReader(new ByteBufferBsonInput(duplicateByteBuf));
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
            duplicateByteBuf.release();
            bsonReader.close();
        }

        return finder.notFound();
    }

    @Override
    public BsonDocument clone() {
        byte[] clonedBytes = new byte[byteBuf.remaining()];
        byteBuf.get(byteBuf.position(), clonedBytes);
        return new RawBsonDocument(clonedBytes);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        return toBsonDocument().equals(o);
    }

    @Override
    public int hashCode() {
        return toBsonDocument().hashCode();
    }

    private BsonDocument toBsonDocument() {
        ByteBuf duplicateByteBuf = byteBuf.duplicate();
        BsonBinaryReader bsonReader = new BsonBinaryReader(new ByteBufferBsonInput(duplicateByteBuf));
        try {
            return new BsonDocumentCodec().decode(bsonReader, DecoderContext.builder().build());
        } finally {
            duplicateByteBuf.release();
            bsonReader.close();
        }
    }

    private BsonValue deserializeBsonValue(final BsonReader bsonReader) {
        return REGISTRY.get(getClassForBsonType(bsonReader.getCurrentBsonType())).decode(bsonReader, DecoderContext.builder().build());
    }

    ByteBufBsonDocument(final ByteBuf byteBuf) {
        this.byteBuf = byteBuf;
    }

    // see https://docs.oracle.com/javase/6/docs/platform/serialization/spec/output.html
    private Object writeReplace() {
        return toBsonDocument();
    }

    // see https://docs.oracle.com/javase/6/docs/platform/serialization/spec/input.html
    private void readObject(final ObjectInputStream stream) throws InvalidObjectException {
        throw new InvalidObjectException("Proxy required");
    }
}
