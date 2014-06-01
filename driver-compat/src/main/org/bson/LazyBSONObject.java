/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.DBPointer;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.RegularExpression;
import org.bson.types.Symbol;
import org.bson.types.Timestamp;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
        this(buffer.array(), callback);
    }

    public LazyBSONObject(final BSONByteBuffer buffer, final int offset, final LazyBSONCallback callback) {
        this(buffer.array(), offset, callback);
    }

    protected int getOffset() {
        return offset;
    }

    protected byte[] getBytes() {
        return bytes;
    }

    @Override
    public Object get(final String key) {
        BSONBinaryReader reader = getBSONReader();
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
    public boolean containsKey(final String key) {
        return containsField(key);
    }

    @Override
    public boolean containsField(final String s) {
        BSONBinaryReader reader = getBSONReader();
        try {
            reader.readStartDocument();
            while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
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
        BSONBinaryReader reader = getBSONReader();
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
                Binary binary = reader.readBinaryData();
                byte binaryType = binary.getType();
                if (binaryType == BSONBinarySubType.BINARY.getValue()
                    || binaryType == BSONBinarySubType.BINARY.getValue()) {
                    return binary.getData();
                } else if (binaryType == BSONBinarySubType.UUID_LEGACY.getValue()) {
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
                return new Date(reader.readDateTime());
            case REGULAR_EXPRESSION:
                RegularExpression regularExpression = reader.readRegularExpression();
                return Pattern.compile(
                                      regularExpression.getPattern(),
                                      BSON.regexFlags(regularExpression.getOptions())
                                      );
            case DB_POINTER:
                DBPointer dbPointer = reader.readDBPointer();
                return callback.createDBRef(dbPointer.getNamespace(), dbPointer.getId());
            case JAVASCRIPT:
                return new Code(reader.readJavaScript());
            case SYMBOL:
                return new Symbol(reader.readSymbol());
            case JAVASCRIPT_WITH_SCOPE:
                return new CodeWScope(reader.readJavaScriptWithScope(), (BSONObject) readDocument(reader));
            case INT32:
                return reader.readInt32();
            case TIMESTAMP:
                Timestamp timestamp = reader.readTimestamp();
                return new BSONTimestamp(timestamp.getTime(), timestamp.getInc());
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

    private Object readArray(final BSONBinaryReader reader) {
        int position = reader.getBuffer().getPosition();
        reader.skipValue();
        return callback.createArray(bytes, offset + position);
    }

    private Object readDocument(final BSONBinaryReader reader) {
        int position = reader.getBuffer().getPosition();
        reader.readStartDocument();
        while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
            reader.skipName();
            reader.skipValue();
        }
        reader.readEndDocument();
        return callback.createObject(bytes, offset + position);
    }

    BSONBinaryReader getBSONReader() {
        ByteBuffer buffer = getBufferForInternalBytes();
        return new BSONBinaryReader(new BasicInputBuffer(new ByteBufNIO(buffer)), true);
    }

    private ByteBuffer getBufferForInternalBytes() {
        ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, bytes.length - offset).slice();
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
        WritableByteChannel channel = Channels.newChannel(os);
        return channel.write(getBufferForInternalBytes());
    }

    public Set<Map.Entry<String, Object>> entrySet() {
        Set<Map.Entry<String, Object>> entries = new LinkedHashSet<Map.Entry<String, Object>>();
        BSONBinaryReader reader = getBSONReader();
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
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        for (final Map.Entry<String, Object> entry : entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        return Collections.unmodifiableMap(map);
    }
}
