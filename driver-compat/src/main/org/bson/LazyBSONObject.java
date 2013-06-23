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

package org.bson;

import org.bson.io.BSONByteBuffer;
import org.bson.io.BasicInputBuffer;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.RegularExpression;
import org.bson.types.Symbol;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.bson.io.Bits.readLong;

public class LazyBSONObject implements BSONObject {
    private final byte[] bytes;
    private final int offset;
    private final LazyBSONCallback callback;


    public LazyBSONObject(final byte[] bytes, final LazyBSONCallback callback) {
        this(bytes, 0, callback);
    }

    public LazyBSONObject(final byte[] bytes, final int offset, final LazyBSONCallback callback) {
        this.bytes = bytes;
        this.callback = callback;
        this.offset = offset;
    }

    public LazyBSONObject(final BSONByteBuffer buffer, final LazyBSONCallback callback) {
        this(buffer.array(), 0, callback);
    }

    public LazyBSONObject(final BSONByteBuffer buffer, final int offset, final LazyBSONCallback callback) {
        this(buffer.array(), offset, callback);
    }

    @Override
    public Object get(final String key) {
        final BSONBinaryReader reader = getBSONReader();
        Object value;
        try {
            reader.readStartDocument();
            value = null;
            while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
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
    @Deprecated
    public boolean containsKey(final String s) {
        return containsField(s);
    }

    @Override
    public boolean containsField(final String s) {
        return keySet().contains(s);
    }

    @Override
    public Set<String> keySet() {
        final Set<String> keys = new HashSet<String>();
        final BSONReader reader = getBSONReader();
        try {
            reader.readStartDocument();
            while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
                keys.add(reader.readName());
                reader.skipValue();
            }
            reader.readEndDocument();
        } finally {
            reader.close();
        }
        return Collections.unmodifiableSet(keys);
    }

    Object readValue(final BSONBinaryReader reader) {
        switch (reader.getCurrentBSONType()) {
            case DOCUMENT:
                return readDocument(reader);
            case ARRAY:
                return readArray(reader);
            case DOUBLE:
                return reader.readDouble();
            case STRING:
                return reader.readString();
            case BINARY:
                final Binary binary = reader.readBinaryData();
                final byte binaryType = binary.getType();
                if (binaryType == BSONBinarySubType.Binary.getValue() ||
                        binaryType == BSONBinarySubType.Binary.getValue()) {
                    return binary.getData();
                } else if (binaryType == BSONBinarySubType.UuidLegacy.getValue()) {
                    return new UUID(readLong(binary.getData(), 0), readLong(binary.getData(), 8));
                } else {
                    return binary;
                }
            case UNDEFINED:
            case NULL:
                return null;
            case OBJECT_ID:
                return reader.readObjectId();
            case BOOLEAN:
                return reader.readBoolean();
            case DATE_TIME:
                return reader.readDateTime();
            case REGULAR_EXPRESSION:
                final RegularExpression regularExpression = reader.readRegularExpression();
                return Pattern.compile(
                        regularExpression.getPattern(),
                        BSON.regexFlags(regularExpression.getOptions())
                );
            case JAVASCRIPT:
                return new Code(reader.readJavaScript());
            case SYMBOL:
                return new Symbol(reader.readSymbol());
            case JAVASCRIPT_WITH_SCOPE:
                return new CodeWScope(reader.readJavaScriptWithScope(), (BSONObject) readDocument(reader));
            case INT32:
                return reader.readInt32();
            case TIMESTAMP:
                return reader.readTimestamp();
            case INT64:
                return reader.readInt64();
            case MIN_KEY:
                reader.readMinKey();
                return new MinKey();
            case MAX_KEY:
                reader.readMaxKey();
                return new MaxKey();
            default:
                throw new IllegalArgumentException("unhandled BSON type: " + reader.getCurrentBSONType());
        }
    }

    Object readArray(final BSONBinaryReader reader) {
        final int position = reader.getBuffer().getPosition();
        reader.skipValue();
        return callback.createArray(bytes, offset + position);
    }

    Object readDocument(final BSONBinaryReader reader) {
        final int position = reader.getBuffer().getPosition();
        reader.skipValue();
        return callback.createObject(bytes, offset + position);
    }

    BSONBinaryReader getBSONReader() {
        final ByteBuffer buffer = getBufferForInternalBytes();
        return new BSONBinaryReader(new BasicInputBuffer(new ByteBufNIO(buffer)), true);
    }

    private ByteBuffer getBufferForInternalBytes() {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, bytes.length - offset).slice();
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.limit(buffer.getInt());
        buffer.rewind();
        return buffer;
    }

    public boolean isEmpty() {
        return keySet().size() == 0;
    }

    public int getBSONSize() {
        return getBufferForInternalBytes().getInt();
    }

    public int pipe(final OutputStream os) throws IOException {
        final WritableByteChannel channel = Channels.newChannel(os);
        return channel.write(getBufferForInternalBytes());
    }

    public Set<Map.Entry<String, Object>> entrySet() {
        final Set<Map.Entry<String, Object>> entries = new HashSet<Map.Entry<String, Object>>();
        final BSONBinaryReader reader = getBSONReader();
        try {
            reader.readStartDocument();
            while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
                entries.add(new AbstractMap.SimpleImmutableEntry<String, Object>(reader.readName(), readValue(reader)));
            }
            reader.readEndDocument();
        } finally {
            reader.close();
        }
        return Collections.unmodifiableSet(entries);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LazyBSONObject that = (LazyBSONObject) o;
        return Arrays.equals(this.bytes, that.bytes);
    }

    /**
     * Returns a JSON serialization of this object
     *
     * @return JSON serialization
     */
    public String toString() {
        return com.mongodb.util.JSON.serialize(this);
    }


    /* ----------------- Unsupported operations --------------------- */

    @Override
    public Object put(final String key, final Object v) {
        throw new UnsupportedOperationException("Object is read only");
    }

    @Override
    public void putAll(final BSONObject o) {
        throw new UnsupportedOperationException("Object is read only");
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void putAll(final Map m) {
        throw new UnsupportedOperationException("Object is read only");
    }

    @Override
    public Object removeField(final String key) {
        throw new UnsupportedOperationException("Object is read only");
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Map toMap() {
        final Map<String, Object> map = new HashMap<String, Object>();
        for (Map.Entry<String, Object> entry : entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }
}
