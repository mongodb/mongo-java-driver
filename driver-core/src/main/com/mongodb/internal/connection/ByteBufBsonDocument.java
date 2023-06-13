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
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.ByteBuf;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.io.ByteBufferBsonInput;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.StringWriter;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.ByteBufBsonHelper.readBsonValue;

final class ByteBufBsonDocument extends BsonDocument {
    private static final long serialVersionUID = 2L;

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
    public BsonBinaryReader asBsonReader() {
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
        try (BsonBinaryReader bsonReader = new BsonBinaryReader(new ByteBufferBsonInput(duplicateByteBuf))) {
            bsonReader.readStartDocument();
            while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                T found = finder.find(duplicateByteBuf, bsonReader);
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
            public Boolean find(final ByteBuf byteBuf, final BsonBinaryReader bsonReader) {
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
            public Integer find(final ByteBuf byteBuf, final BsonBinaryReader bsonReader) {
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
        return new ByteBufBsonDocumentEntrySet();
    }

    @Override
    public Collection<BsonValue> values() {
        return new ByteBufBsonDocumentValuesCollection();
    }

    @Override
    public Set<String> keySet() {
        return new ByteBufBsonDocumentKeySet();
    }

    @Override
    public boolean containsKey(final Object key) {
        if (key == null) {
            throw new IllegalArgumentException("key can not be null");
        }

        Boolean containsKey = findInDocument(new Finder<Boolean>() {
            @Override
            public Boolean find(final ByteBuf byteBuf, final BsonBinaryReader bsonReader) {
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
            public Boolean find(final ByteBuf byteBuf, final BsonBinaryReader bsonReader) {
                bsonReader.skipName();
                if (readBsonValue(byteBuf, bsonReader).equals(value)) {
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
            public BsonValue find(final ByteBuf byteBuf, final BsonBinaryReader bsonReader) {
                if (bsonReader.readName().equals(key)) {
                    return readBsonValue(byteBuf, bsonReader);
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
            public String find(final ByteBuf byteBuf, final BsonBinaryReader bsonReader) {
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
        T find(ByteBuf byteBuf, BsonBinaryReader bsonReader);
        @Nullable
        T notFound();
    }

    // see https://docs.oracle.com/javase/6/docs/platform/serialization/spec/output.html
    private Object writeReplace() {
        return toBaseBsonDocument();
    }

    // see https://docs.oracle.com/javase/6/docs/platform/serialization/spec/input.html
    private void readObject(final ObjectInputStream stream) throws InvalidObjectException {
        throw new InvalidObjectException("Proxy required");
    }

    private class ByteBufBsonDocumentEntrySet extends AbstractSet<Entry<String, BsonValue>> {
        @Override
        public Iterator<Entry<String, BsonValue>> iterator() {
            return new Iterator<Entry<String, BsonValue>>() {
                private final ByteBuf duplicatedByteBuf = byteBuf.duplicate();
                private final BsonBinaryReader bsonReader;

                {
                    bsonReader = new BsonBinaryReader(new ByteBufferBsonInput(duplicatedByteBuf));
                    bsonReader.readStartDocument();
                    bsonReader.readBsonType();
                }

                @Override
                public boolean hasNext() {
                    return bsonReader.getCurrentBsonType() != BsonType.END_OF_DOCUMENT;
                }

                @Override
                public Entry<String, BsonValue> next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    String key = bsonReader.readName();
                    BsonValue value = readBsonValue(duplicatedByteBuf, bsonReader);
                    bsonReader.readBsonType();
                    return new AbstractMap.SimpleEntry<>(key, value);
                }

            };
        }

        @Override
        public boolean isEmpty() {
            return !iterator().hasNext();
        }

        @Override
        public int size() {
            return ByteBufBsonDocument.this.size();
        }
    }

    private class ByteBufBsonDocumentKeySet extends AbstractSet<String> {
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
        private final Set<Entry<String, BsonValue>> entrySet = new ByteBufBsonDocumentEntrySet();

        @Override
        public Iterator<String> iterator() {
            final Iterator<Entry<String, BsonValue>> entrySetIterator = entrySet.iterator();
            return new Iterator<String>() {
                @Override
                public boolean hasNext() {
                    return entrySetIterator.hasNext();
                }

                @Override
                public String next() {
                    return entrySetIterator.next().getKey();
                }
            };
        }

        @Override
        public boolean isEmpty() {
            return entrySet.isEmpty();
        }

        @Override
        public int size() {
            return entrySet.size();
        }
    }

    private class ByteBufBsonDocumentValuesCollection extends AbstractCollection<BsonValue> {
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
        private final Set<Entry<String, BsonValue>> entrySet = new ByteBufBsonDocumentEntrySet();

        @Override
        public Iterator<BsonValue> iterator() {
            final Iterator<Entry<String, BsonValue>> entrySetIterator = entrySet.iterator();
            return new Iterator<BsonValue>() {
                @Override
                public boolean hasNext() {
                    return entrySetIterator.hasNext();
                }

                @Override
                public BsonValue next() {
                    return entrySetIterator.next().getValue();
                }
            };
        }

        @Override
        public boolean isEmpty() {
            return entrySet.isEmpty();
        }
        @Override
        public int size() {
            return entrySet.size();
        }
    }
}
