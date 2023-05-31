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
import org.bson.json.JsonMode;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.notNull;
import static org.bson.codecs.BsonValueCodecProvider.getClassForBsonType;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

final class ByteBufBsonDocument extends BsonDocument {
    private static final long serialVersionUID = 2L;

    private static final CodecRegistry REGISTRY = fromProviders(new BsonValueCodecProvider());

    private final transient ByteBuf byteBuf;

    static List<ByteBufBsonDocument> createList(final ByteBufferBsonOutput bsonOutput, final int startPosition) {
        List<ByteBuf> duplicateByteBuffers = bsonOutput.getByteBuffers();
        CompositeByteBuf outputByteBuf = new CompositeByteBuf(duplicateByteBuffers);
        outputByteBuf.position(startPosition);
        List<ByteBufBsonDocument> documents = new ArrayList<>();
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

    static ByteBufBsonDocument createOne(final ByteBufferBsonOutput bsonOutput, final int startPosition) {
        List<ByteBuf> duplicateByteBuffers = bsonOutput.getByteBuffers();
        CompositeByteBuf outputByteBuf = new CompositeByteBuf(duplicateByteBuffers);
        outputByteBuf.position(startPosition);
        int documentSizeInBytes = outputByteBuf.getInt();
        ByteBuf slice = outputByteBuf.duplicate();
        slice.position(startPosition);
        slice.limit(startPosition + documentSizeInBytes);
        for (ByteBuf byteBuffer : duplicateByteBuffers) {
            byteBuffer.release();
        }
        return new ByteBufBsonDocument(slice);
    }

    @Override
    public String toJson() {
        return toJson(JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build());
    }

    @Override
    public String toJson(final JsonWriterSettings settings) {
        StringWriter stringWriter = new StringWriter();
        JsonWriter jsonWriter = new JsonWriter(stringWriter, settings);
        ByteBuf duplicate = byteBuf.duplicate();
        try (BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(duplicate))) {
            jsonWriter.pipe(reader);
            return stringWriter.toString();
        } finally {
            duplicate.release();
        }
    }

    @Override
    public BsonReader asBsonReader() {
        return new BsonBinaryReader(new ByteBufferBsonInput(byteBuf.duplicate()));
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public BsonDocument clone() {
        byte[] clonedBytes = new byte[byteBuf.remaining()];
        byteBuf.get(byteBuf.position(), clonedBytes);
        return new RawBsonDocument(clonedBytes);
    }

    @Nullable
    <T> T findInDocument(final Finder<T> finder) {
        ByteBuf duplicateByteBuf = byteBuf.duplicate();
        try (BsonBinaryReader bsonReader = new BsonBinaryReader(new ByteBufferBsonInput(byteBuf.duplicate()))) {
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
        }

        return finder.notFound();
    }

    int getSizeInBytes() {
        return byteBuf.getInt(byteBuf.position());
    }

    BsonDocument toBaseBsonDocument() {
        ByteBuf duplicateByteBuf = byteBuf.duplicate();
        try (BsonBinaryReader bsonReader = new BsonBinaryReader(new ByteBufferBsonInput(duplicateByteBuf))) {
            return new BsonDocumentCodec().decode(bsonReader, DecoderContext.builder().build());
        } finally {
            duplicateByteBuf.release();
        }
    }

    ByteBufBsonDocument(final ByteBuf byteBuf) {
        this.byteBuf = byteBuf;
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("ByteBufBsonDocument instances are immutable");
    }

    @Override
    public BsonValue put(final String key, final BsonValue value) {
        throw new UnsupportedOperationException("ByteBufBsonDocument instances are immutable");
    }

    @Override
    public BsonDocument append(final String key, final BsonValue value) {
        throw new UnsupportedOperationException("ByteBufBsonDocument instances are immutable");
    }

    @Override
    public void putAll(final Map<? extends String, ? extends BsonValue> m) {
        throw new UnsupportedOperationException("ByteBufBsonDocument instances are immutable");
    }

    @Override
    public BsonValue remove(final Object key) {
        throw new UnsupportedOperationException("ByteBufBsonDocument instances are immutable");
    }

    @Override
    public boolean isEmpty() {
        return assertNotNull(findInDocument(new Finder<Boolean>() {
            @Override
            public Boolean find(final BsonReader bsonReader) {
                return false;
            }

            @Override
            public Boolean notFound() {
                return true;
            }
        }));
    }

    @Override
    public int size() {
        return assertNotNull(findInDocument(new Finder<Integer>() {
            private int size;

            @Override
            @Nullable
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
        }));
    }

    @Override
    public Set<Entry<String, BsonValue>> entrySet() {
        return toBaseBsonDocument().entrySet();
    }

    @Override
    public Collection<BsonValue> values() {
        return toBaseBsonDocument().values();
    }

    @Override
    public Set<String> keySet() {
        return toBaseBsonDocument().keySet();
    }

    @Override
    public boolean containsKey(final Object key) {
        if (key == null) {
            throw new IllegalArgumentException("key can not be null");
        }

        Boolean containsKey = findInDocument(new Finder<Boolean>() {
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
        return containsKey != null ? containsKey : false;
    }

    @Override
    public boolean containsValue(final Object value) {
        Boolean containsValue = findInDocument(new Finder<Boolean>() {
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
        return containsValue != null ? containsValue : false;
    }

    @Nullable
    @Override
    public BsonValue get(final Object key) {
        notNull("key", key);
        return findInDocument(new Finder<BsonValue>() {
            @Override
            public BsonValue find(final BsonReader bsonReader) {
                if (bsonReader.readName().equals(key)) {
                    return deserializeBsonValue(bsonReader);
                }
                bsonReader.skipValue();
                return null;
            }

            @Nullable
            @Override
            public BsonValue notFound() {
                return null;
            }
        });
    }

    /**
     * Gets the first key in this document.
     *
     * @return the first key in this document
     * @throws java.util.NoSuchElementException if the document is empty
     */
    public String getFirstKey() {
        return assertNotNull(findInDocument(new Finder<String>() {
            @Override
            public String find(final BsonReader bsonReader) {
                return bsonReader.readName();
            }

            @Override
            public String notFound() {
                throw new NoSuchElementException();
            }
        }));
    }

    private interface Finder<T> {
        @Nullable
        T find(BsonReader bsonReader);
        @Nullable
        T notFound();
    }

    private BsonValue deserializeBsonValue(final BsonReader bsonReader) {
        return REGISTRY.get(getClassForBsonType(bsonReader.getCurrentBsonType())).decode(bsonReader, DecoderContext.builder().build());
    }

    // see https://docs.oracle.com/javase/6/docs/platform/serialization/spec/output.html
    Object writeReplace() {
        return toBaseBsonDocument();
    }

    // see https://docs.oracle.com/javase/6/docs/platform/serialization/spec/input.html
    private void readObject(final ObjectInputStream stream) throws InvalidObjectException {
        throw new InvalidObjectException("Proxy required");
    }
}
