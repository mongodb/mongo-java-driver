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

package org.bson;

import org.bson.io.ByteBufferBsonInput;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractList;
import java.util.Collection;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import static org.bson.assertions.Assertions.isTrueArgument;
import static org.bson.assertions.Assertions.notNull;

/**
 * An immutable BSON array that is represented using only the raw bytes.
 *
 * @since 3.7
 */
public class RawBsonArray extends BsonArray implements Serializable {
    private static final long serialVersionUID = 2L;
    private static final String IMMUTABLE_MSG = "RawBsonArray instances are immutable";

    private final transient RawBsonArrayList delegate;

    /**
     * Constructs a new instance with the given byte array. Note that it does not make a copy of the array, so do not modify it after
     * passing it to this constructor.
     *
     * @param bytes the bytes representing a BSON document. Note that the byte array is NOT copied, so care must be taken not to modify it
     *              after passing it to this construction, unless of course that is your intention.
     */
    public RawBsonArray(final byte[] bytes) {
        this(notNull("bytes", bytes), 0, bytes.length);
    }

    /**
     * Constructs a new instance with the given byte array, offset, and length. Note that it does not make a copy of the array, so do not
     * modify it after passing it to this constructor.
     *
     * @param bytes the bytes representing a BSON document.  Note that the byte array is NOT copied, so care must be taken not to modify it
     *              after passing it to this construction, unless of course that is your intention.
     * @param offset the offset into the byte array
     * @param length the length of the subarray to use
     */
    public RawBsonArray(final byte[] bytes, final int offset, final int length) {
        this(new RawBsonArrayList(bytes, offset, length));
    }

    private RawBsonArray(final RawBsonArrayList values) {
        super(values, false);
        this.delegate = values;
    }

    ByteBuf getByteBuffer() {
        return delegate.getByteBuffer();
    }

    @Override
    public boolean add(final BsonValue bsonValue) {
        throw new UnsupportedOperationException(IMMUTABLE_MSG);
    }

    @Override
    public boolean remove(final Object o) {
        throw new UnsupportedOperationException(IMMUTABLE_MSG);
    }

    @Override
    public boolean addAll(final Collection<? extends BsonValue> c) {
        throw new UnsupportedOperationException(IMMUTABLE_MSG);
    }

    @Override
    public boolean addAll(final int index, final Collection<? extends BsonValue> c) {
        throw new UnsupportedOperationException(IMMUTABLE_MSG);
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
        throw new UnsupportedOperationException(IMMUTABLE_MSG);
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
        throw new UnsupportedOperationException(IMMUTABLE_MSG);
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException(IMMUTABLE_MSG);
    }

    @Override
    public BsonValue set(final int index, final BsonValue element) {
        throw new UnsupportedOperationException(IMMUTABLE_MSG);
    }

    @Override
    public void add(final int index, final BsonValue element) {
        throw new UnsupportedOperationException(IMMUTABLE_MSG);
    }

    @Override
    public BsonValue remove(final int index) {
        throw new UnsupportedOperationException(IMMUTABLE_MSG);
    }

    @Override
    public BsonArray clone() {
        return new RawBsonArray(delegate.bytes.clone(), delegate.offset, delegate.length);
    }

    @Override
    public boolean equals(final Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    /**
     * Write the replacement object.
     *
     * <p>
     * See https://docs.oracle.com/javase/6/docs/platform/serialization/spec/output.html
     * </p>
     *
     * @return a proxy for the document
     */
    private Object writeReplace() {
        return new SerializationProxy(delegate.bytes, delegate.offset, delegate.length);
    }

    /**
     * Prevent normal serialization.
     *
     * <p>
     * See https://docs.oracle.com/javase/6/docs/platform/serialization/spec/input.html
     * </p>
     *
     * @param stream the stream
     * @throws InvalidObjectException in all cases
     */
    private void readObject(final ObjectInputStream stream) throws InvalidObjectException {
        throw new InvalidObjectException("Proxy required");
    }

    private static class SerializationProxy implements Serializable {
        private static final long serialVersionUID = 1L;

        private final byte[] bytes;

        SerializationProxy(final byte[] bytes, final int offset, final int length) {
            if (bytes.length == length) {
                this.bytes = bytes;
            } else {
                this.bytes = new byte[length];
                System.arraycopy(bytes, offset, this.bytes, 0, length);
            }
        }

        private Object readResolve() {
            return new RawBsonArray(bytes);
        }
    }

    static class RawBsonArrayList extends AbstractList<BsonValue> {
        private static final int MIN_BSON_ARRAY_SIZE = 5;
        private Integer cachedSize;
        private final byte[] bytes;
        private final int offset;
        private final int length;

        RawBsonArrayList(final byte[] bytes, final int offset, final int length) {
            notNull("bytes", bytes);
            isTrueArgument("offset >= 0", offset >= 0);
            isTrueArgument("offset < bytes.length", offset < bytes.length);
            isTrueArgument("length <= bytes.length - offset", length <= bytes.length - offset);
            isTrueArgument("length >= 5", length >= MIN_BSON_ARRAY_SIZE);
            this.bytes = bytes;
            this.offset = offset;
            this.length = length;
        }

        @Override
        public BsonValue get(final int index) {
            if (index < 0) {
                throw new IndexOutOfBoundsException();
            }
            int curIndex = 0;
            BsonBinaryReader bsonReader = createReader();
            try {
                bsonReader.readStartDocument();
                while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    bsonReader.skipName();
                    if (curIndex == index) {
                        return RawBsonValueHelper.decode(bytes, bsonReader);
                    }
                    bsonReader.skipValue();
                    curIndex++;
                }
                bsonReader.readEndDocument();
            } finally {
                bsonReader.close();
            }
            throw new IndexOutOfBoundsException();
        }

        @Override
        public int size() {
            if (cachedSize != null) {
                return cachedSize;
            }
            int size = 0;
            BsonBinaryReader bsonReader = createReader();
            try {
                bsonReader.readStartDocument();
                while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    size++;
                    bsonReader.readName();
                    bsonReader.skipValue();
                }
                bsonReader.readEndDocument();
            } finally {
                bsonReader.close();
            }
            cachedSize = size;
            return cachedSize;
        }

        @Override
        public Iterator<BsonValue> iterator() {
            return new Itr();
        }

        @Override
        public ListIterator<BsonValue> listIterator() {
            return new ListItr(0);
        }

        @Override
        public ListIterator<BsonValue> listIterator(final int index) {
            return new ListItr(index);
        }

        private class Itr implements Iterator<BsonValue> {
            private int cursor = 0;
            private BsonBinaryReader bsonReader;
            private int currentPosition = 0;

            Itr() {
                this(0);
            }

            Itr(final int cursorPosition) {
                setIterator(cursorPosition);
            }

            public boolean hasNext() {
                boolean hasNext = cursor != size();
                if (!hasNext) {
                    bsonReader.close();
                }
                return hasNext;
            }

            public BsonValue next() {
                while (cursor > currentPosition && bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    bsonReader.skipName();
                    bsonReader.skipValue();
                    currentPosition++;
                }

                if (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    bsonReader.skipName();
                    cursor += 1;
                    currentPosition = cursor;
                    return RawBsonValueHelper.decode(bytes, bsonReader);
                } else {
                    bsonReader.close();
                    throw new NoSuchElementException();
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException(IMMUTABLE_MSG);
            }

            public int getCursor() {
                return cursor;
            }

            public void setCursor(final int cursor) {
                this.cursor = cursor;
            }

            void setIterator(final int cursorPosition) {
                cursor = cursorPosition;
                currentPosition = 0;
                if (bsonReader != null) {
                    bsonReader.close();
                }
                bsonReader = createReader();
                bsonReader.readStartDocument();
            }
        }

        private class ListItr extends Itr implements ListIterator<BsonValue> {
            ListItr(final int index) {
                super(index);
            }

            public boolean hasPrevious() {
                return getCursor() != 0;
            }

            public BsonValue previous() {
                try {
                    BsonValue previous = get(previousIndex());
                    setIterator(previousIndex());
                    return previous;
                } catch (IndexOutOfBoundsException e) {
                    throw new NoSuchElementException();
                }
            }

            public int nextIndex() {
                return getCursor();
            }

            public int previousIndex() {
                return getCursor() - 1;
            }

            @Override
            public void set(final BsonValue bsonValue) {
                throw new UnsupportedOperationException(IMMUTABLE_MSG);
            }

            @Override
            public void add(final BsonValue bsonValue) {
                throw new UnsupportedOperationException(IMMUTABLE_MSG);
            }
        }

        private BsonBinaryReader createReader() {
            return new BsonBinaryReader(new ByteBufferBsonInput(getByteBuffer()));
        }

        ByteBuf getByteBuffer() {
            ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, length);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            return new ByteBufNIO(buffer);
        }
    }
}
