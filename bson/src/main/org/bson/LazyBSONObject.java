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

import org.bson.codecs.DecoderContext;
import org.bson.codecs.UuidCodec;
import org.bson.io.ByteBufferBsonInput;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.Symbol;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.bson.BsonBinarySubType.BINARY;
import static org.bson.BsonBinarySubType.OLD_BINARY;

/**
 * An immutable {@code BSONObject} backed by a byte buffer that lazily provides keys and values on request. This is useful for transferring
 * BSON documents between servers when you don't want to pay the performance penalty of encoding or decoding them fully.
 */
public class LazyBSONObject implements BSONObject {
    private final byte[] bytes;
    private final int offset;
    private final LazyBSONCallback callback;


    /**
     * Construct an instance.
     *
     * @param bytes the raw bytes
     * @param callback the callback to use to construct nested values
     */
    public LazyBSONObject(final byte[] bytes, final LazyBSONCallback callback) {
        this(bytes, 0, callback);
    }

    /**
     * Construct an instance.
     *
     * @param bytes the raw bytes
     * @param offset the offset into the raw bytes representing the start of the document
     * @param callback the callback to use to construct nested values
     */
    public LazyBSONObject(final byte[] bytes, final int offset, final LazyBSONCallback callback) {
        this.bytes = bytes;
        this.callback = callback;
        this.offset = offset;
    }

    /**
     * Gets the offset into the raw bytes representing the start of the document
     *
     * @return the offset
     */
    protected int getOffset() {
        return offset;
    }

    /**
     * Gets the raw bytes.
     *
     * @return the raw bytes
     */
    protected byte[] getBytes() {
        return bytes;
    }

    @Override
    public Object get(final String key) {
        BsonBinaryReader reader = getBsonReader();
        Object value;
        try {
            reader.readStartDocument();
            value = null;
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                if (key.equals(reader.readName())) {
                    value = readValue(reader);
                    break;
                } else {
                    reader.skipValue();
                }
            }
        } finally {
            reader.close();
        }
        return value;
    }

    @Override
    public boolean containsField(final String s) {
        BsonBinaryReader reader = getBsonReader();
        try {
            reader.readStartDocument();
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                if (reader.readName().equals(s)) {
                    return true;
                } else {
                    reader.skipValue();
                }
            }
        } finally {
            reader.close();
        }
        return false;
    }

    @Override
    public Set<String> keySet() {
        Set<String> keys = new LinkedHashSet<String>();
        BsonBinaryReader reader = getBsonReader();
        try {
            reader.readStartDocument();
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                keys.add(reader.readName());
                reader.skipValue();
            }
            reader.readEndDocument();
        } finally {
            reader.close();
        }
        return Collections.unmodifiableSet(keys);
    }

    Object readValue(final BsonBinaryReader reader) {
        switch (reader.getCurrentBsonType()) {
            case DOCUMENT:
                return readDocument(reader);
            case ARRAY:
                return readArray(reader);
            case DOUBLE:
                return reader.readDouble();
            case STRING:
                return reader.readString();
            case BINARY:
                byte binarySubType = reader.peekBinarySubType();
                if (BsonBinarySubType.isUuid(binarySubType) && reader.peekBinarySize() == 16) {
                    return new UuidCodec(UuidRepresentation.JAVA_LEGACY).decode(reader, DecoderContext.builder().build());
                }
                BsonBinary binary = reader.readBinaryData();
                if (binarySubType == BINARY.getValue() || binarySubType == OLD_BINARY.getValue()) {
                    return binary.getData();
                } else {
                    return new Binary(binary.getType(), binary.getData());
                }
            case NULL:
                reader.readNull();
                return null;
            case UNDEFINED:
                reader.readUndefined();
                return null;
            case OBJECT_ID:
                return reader.readObjectId();
            case BOOLEAN:
                return reader.readBoolean();
            case DATE_TIME:
                return new Date(reader.readDateTime());
            case REGULAR_EXPRESSION:
                BsonRegularExpression regularExpression = reader.readRegularExpression();
                return Pattern.compile(
                                      regularExpression.getPattern(),
                                      BSON.regexFlags(regularExpression.getOptions())
                                      );
            case DB_POINTER:
                BsonDbPointer dbPointer = reader.readDBPointer();
                return callback.createDBRef(dbPointer.getNamespace(), dbPointer.getId());
            case JAVASCRIPT:
                return new Code(reader.readJavaScript());
            case SYMBOL:
                return new Symbol(reader.readSymbol());
            case JAVASCRIPT_WITH_SCOPE:
                return new CodeWScope(reader.readJavaScriptWithScope(), (BSONObject) readJavaScriptWithScopeDocument(reader));
            case INT32:
                return reader.readInt32();
            case TIMESTAMP:
                BsonTimestamp timestamp = reader.readTimestamp();
                return new BSONTimestamp(timestamp.getTime(), timestamp.getInc());
            case INT64:
                return reader.readInt64();
            case DECIMAL128:
                return reader.readDecimal128();
            case MIN_KEY:
                reader.readMinKey();
                return new MinKey();
            case MAX_KEY:
                reader.readMaxKey();
                return new MaxKey();
            default:
                throw new IllegalArgumentException("unhandled BSON type: " + reader.getCurrentBsonType());
        }
    }

    private Object readArray(final BsonBinaryReader reader) {
        int position = reader.getBsonInput().getPosition();
        reader.skipValue();
        return callback.createArray(bytes, offset + position);
    }

    private Object readDocument(final BsonBinaryReader reader) {
        int position = reader.getBsonInput().getPosition();
        reader.skipValue();
        return callback.createObject(bytes, offset + position);
    }

    private Object readJavaScriptWithScopeDocument(final BsonBinaryReader reader) {
        int position = reader.getBsonInput().getPosition();
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            reader.skipName();
            reader.skipValue();
        }
        reader.readEndDocument();
        return callback.createObject(bytes, offset + position);
    }

    BsonBinaryReader getBsonReader() {
        ByteBuffer buffer = getBufferForInternalBytes();
        return new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(buffer)));
    }

    private ByteBuffer getBufferForInternalBytes() {
        ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, bytes.length - offset).slice();
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        ((Buffer) buffer).limit(buffer.getInt());
        ((Buffer) buffer).rewind();
        return buffer;
    }

    /**
     * Gets whether this is an empty {@code BSONObject}.
     *
     * @return true if this has no keys
     */
    public boolean isEmpty() {
        return keySet().size() == 0;
    }

    /**
     * Gets the size in bytes of the BSON document.
     *
     * @return the size in bytes
     */
    public int getBSONSize() {
        return getBufferForInternalBytes().getInt();
    }

    /**
     * Pipe the raw bytes into the given output stream.
     *
     * @param os the output stream
     * @return the number of bytes written
     * @throws IOException any IOException thrown by the output stream
     */
    public int pipe(final OutputStream os) throws IOException {
        WritableByteChannel channel = Channels.newChannel(os);
        return channel.write(getBufferForInternalBytes());
    }

    /**
     * Gets the entry set for all the key/value pairs in this {@code BSONObject}.  The returned set is immutable.
     *
     * @return then entry set
     */
    public Set<Map.Entry<String, Object>> entrySet() {
        final List<Map.Entry<String, Object>> entries = new ArrayList<Map.Entry<String, Object>>();
        BsonBinaryReader reader = getBsonReader();
        try {
            reader.readStartDocument();
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                entries.add(new AbstractMap.SimpleImmutableEntry<String, Object>(reader.readName(), readValue(reader)));
            }
            reader.readEndDocument();
        } finally {
            reader.close();
        }
        return new Set<Map.Entry<String, Object>>() {
            @Override
            public int size() {
                return entries.size();
            }

            @Override
            public boolean isEmpty() {
                return entries.isEmpty();
            }

            @Override
            public Iterator<Map.Entry<String, Object>> iterator() {
                return entries.iterator();
            }

            @Override
            public Object[] toArray() {
                return entries.toArray();
            }

            @Override
            public <T> T[] toArray(final T[] a) {
                return entries.toArray(a);
            }

            @Override
            public boolean contains(final Object o) {
                return entries.contains(o);
            }

            @Override
            public boolean containsAll(final Collection<?> c) {
                return entries.containsAll(c);
            }

            @Override
            public boolean add(final Map.Entry<String, Object> stringObjectEntry) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean remove(final Object o) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean addAll(final Collection<? extends Map.Entry<String, Object>> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean retainAll(final Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean removeAll(final Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void clear() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public int hashCode() {
        int result = 1;
        int size = getBSONSize();
        for (int i = offset; i < offset + size; i++) {
            result = 31 * result + bytes[i];
        }
        return result;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LazyBSONObject other = (LazyBSONObject) o;

        if (this.bytes == other.bytes && this.offset == other.offset) {
            return true;
        }
        if (this.bytes == null || other.bytes == null) {
            return false;
        }

        if (this.bytes.length == 0 || other.bytes.length == 0) {
            return false;
        }

        //comparing document length
        int length = this.bytes[this.offset];
        if (other.bytes[other.offset] != length) {
            return false;
        }

        //comparing document contents
        for (int i = 0; i < length; i++) {
            if (this.bytes[this.offset + i] != other.bytes[other.offset + i]) {
                return false;
            }
        }

        return true;
    }


    /* ----------------- Unsupported operations --------------------- */

    /**
     * Always throws {@code UnsupportedOperationException}.
     *
     * @param key Name to set
     * @param v   Corresponding value
     * @return will not return normally
     * @throws java.lang.UnsupportedOperationException the object is read only
     */
    @Override
    public Object put(final String key, final Object v) {
        throw new UnsupportedOperationException("Object is read only");
    }

    /**
     * Always throws {@code UnsupportedOperationException}.
     *
     * @param o the object
     * @throws java.lang.UnsupportedOperationException the object is read only
     */
    @Override
    public void putAll(final BSONObject o) {
        throw new UnsupportedOperationException("Object is read only");
    }

    /**
     * Always throws {@code UnsupportedOperationException}.
     *
     * @param m the map
     * @throws java.lang.UnsupportedOperationException the object is read only
     */@Override
    @SuppressWarnings("rawtypes")
    public void putAll(final Map m) {
        throw new UnsupportedOperationException("Object is read only");
    }

    /**
     * Always throws {@code UnsupportedOperationException}.
     *
     * @param key The name of the field to remove
     * @return will not return normally
     * @throws java.lang.UnsupportedOperationException the object is read only
     */
    @Override
    public Object removeField(final String key) {
        throw new UnsupportedOperationException("Object is read only");
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Map toMap() {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        for (final Map.Entry<String, Object> entry : entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        return Collections.unmodifiableMap(map);
    }
}
