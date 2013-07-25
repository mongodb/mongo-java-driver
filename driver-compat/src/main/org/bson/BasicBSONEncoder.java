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

import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.RegularExpression;
import org.bson.types.Symbol;

import java.lang.reflect.Array;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static org.bson.BSON.regexFlags;

/**
 * This is meant to be pooled or cached
 * There is some per instance memory for string conversion, etc...
 */
public class BasicBSONEncoder implements BSONEncoder {

    private BSONBinaryWriter bsonWriter;

    @Override
    public byte[] encode(final BSONObject document) {
        final OutputBuffer outputBuffer = new BasicOutputBuffer();
        set(outputBuffer);
        putObject(document);
        done();
        return outputBuffer.toByteArray();
    }

    @Override
    public void done() {
        this.bsonWriter.close();
        this.bsonWriter = null;
    }

    @Override
    public void set(final OutputBuffer buffer) {
        if (this.bsonWriter != null) {
            throw new IllegalStateException("Performing another operation at this moment");
        }
        this.bsonWriter = new BSONBinaryWriter(buffer, false);
    }

    protected OutputBuffer getOutputBuffer() {
        return bsonWriter.getBuffer();
    }

    protected BSONBinaryWriter getBsonWriter() {
        return bsonWriter;
    }

    /**
     * Encodes a {@code BSONObject}.
     * This is for the higher level api calls.
     *
     * @param document the document to encode
     * @return the number of characters in the encoding
     */
    @Override
    public int putObject(final BSONObject document) {
        final int startPosition = getOutputBuffer().getPosition();
        bsonWriter.writeStartDocument();
        for (final String key : document.keySet()) {
            putObjectField(key, document.get(key));
        }
        bsonWriter.writeEndDocument();
        return getOutputBuffer().getPosition() - startPosition;
    }

    protected void putName(final String name) {
        if (bsonWriter.getState() == BSONWriter.State.NAME) {
            bsonWriter.writeName(name);
        }
    }

    protected void putObjectField(final String name, final Object initialValue) {
        if ("_transientFields".equals(name)) {
            return;
        }
        if (name.contains("\0")) {
            throw new IllegalArgumentException("Document field names can't have a NULL character. (Bad Key: '" + name + "')");
        }

        if ("$where".equals(name) && initialValue instanceof String) {
            putCode(name, new Code((String) initialValue));
        }

        final Object value = BSON.applyEncodingHooks(initialValue);

        if (value == null) {
            putNull(name);
        } else if (value instanceof Date) {
            putDate(name, (Date) value);
        } else if (value instanceof Number) {
            putNumber(name, (Number) value);
        } else if (value instanceof Character) {
            putString(name, value.toString());
        } else if (value instanceof String) {
            putString(name, value.toString());
        } else if (value instanceof ObjectId) {
            putObjectId(name, (ObjectId) value);
        } else if (value instanceof Boolean) {
            putBoolean(name, (Boolean) value);
        } else if (value instanceof Pattern) {
            putPattern(name, (Pattern) value);
        } else if (value instanceof Iterable) {
            putIterable(name, (Iterable) value);
        } else if (value instanceof BSONObject) {
            putObject(name, (BSONObject) value);
        } else if (value instanceof Map) {
            putMap(name, (Map) value);
        } else if (value instanceof byte[]) {
            putBinary(name, (byte[]) value);
        } else if (value instanceof Binary) {
            putBinary(name, (Binary) value);
        } else if (value instanceof UUID) {
            putUUID(name, (UUID) value);
        } else if (value.getClass().isArray()) {
            putArray(name, value);
        } else if (value instanceof Symbol) {
            putSymbol(name, (Symbol) value);
        } else if (value instanceof BSONTimestamp) {
            putTimestamp(name, (BSONTimestamp) value);
        } else if (value instanceof CodeWScope) {
            putCodeWScope(name, (CodeWScope) value);
        } else if (value instanceof Code) {
            putCode(name, (Code) value);
        } else if (value instanceof MinKey) {
            putMinKey(name);
        } else if (value instanceof MaxKey) {
            putMaxKey(name);
        } else if (putSpecial(name, value)) {
            // no-op
        } else {
            throw new IllegalArgumentException("Can't serialize " + value.getClass());
        }

    }

    protected void putNull(final String name) {
        putName(name);
        bsonWriter.writeNull();
    }

    protected void putUndefined(final String name) {
        putName(name);
        bsonWriter.writeUndefined();
    }

    protected void putTimestamp(final String name, final BSONTimestamp timestamp) {
        putName(name);
        bsonWriter.writeTimestamp(timestamp);
    }

    protected void putCode(final String name, final Code code) {
        putName(name);
        bsonWriter.writeJavaScript(code.getCode());
    }

    protected void putCodeWScope(final String name, final CodeWScope codeWScope) {
        putName(name);
        bsonWriter.writeJavaScriptWithScope(codeWScope.getCode());
        putObject(codeWScope.getScope());
    }

    protected void putBoolean(final String name, final Boolean value) {
        putName(name);
        bsonWriter.writeBoolean(value);
    }

    protected void putDate(final String name, final Date date) {
        putName(name);
        bsonWriter.writeDateTime(date.getTime());
    }

    protected void putNumber(final String name, final Number n) {
        putName(name);
        if (n instanceof Integer || n instanceof Short || n instanceof Byte || n instanceof AtomicInteger) {
            bsonWriter.writeInt32(n.intValue());
        } else if (n instanceof Long || n instanceof AtomicLong) {
            bsonWriter.writeInt64(n.longValue());
        } else if (n instanceof Float || n instanceof Double) {
            bsonWriter.writeDouble(n.doubleValue());
        } else {
            throw new IllegalArgumentException("Can't serialize " + n.getClass());
        }
    }

    protected void putBinary(final String name, final byte[] bytes) {
        putName(name);
        bsonWriter.writeBinaryData(new Binary(bytes));
    }

    protected void putBinary(final String name, final Binary binary) {
        putName(name);
        bsonWriter.writeBinaryData(binary);
    }

    protected void putUUID(final String name, final UUID uuid) {
        putName(name);
        final byte[] bytes = new byte[16];
        writeLongToArrayLittleEndian(bytes, 0, uuid.getMostSignificantBits());
        writeLongToArrayLittleEndian(bytes, 8, uuid.getLeastSignificantBits());
        bsonWriter.writeBinaryData(new Binary(BSONBinarySubType.UuidLegacy, bytes));
    }

    protected void putSymbol(final String name, final Symbol symbol) {
        putName(name);
        bsonWriter.writeSymbol(symbol.getSymbol());
    }

    protected void putString(final String name, final String value) {
        putName(name);
        bsonWriter.writeString(value);
    }

    protected void putPattern(final String name, final Pattern value) {
        putName(name);
        bsonWriter.writeRegularExpression(new RegularExpression(value.pattern(), regexFlags(value.flags())));
    }

    protected void putObjectId(final String name, final ObjectId objectId) {
        putName(name);
        bsonWriter.writeObjectId(objectId);
    }

    protected void putArray(final String name, final Object object) {
        putName(name);
        bsonWriter.writeStartArray();
        if (object instanceof int[]) {
            for (int i : (int[]) object) {
                bsonWriter.writeInt32(i);
            }
        } else if (object instanceof long[]) {
            for (long i : (long[]) object) {
                bsonWriter.writeInt64(i);
            }
        } else if (object instanceof float[]) {
            for (float i : (float[]) object) {
                bsonWriter.writeDouble(i);
            }
        } else if (object instanceof short[]) {
            for (short i : (short[]) object) {
                bsonWriter.writeInt32(i);
            }
        } else if (object instanceof byte[]) {
            for (byte i : (byte[]) object) {
                bsonWriter.writeInt32(i);
            }
        } else if (object instanceof double[]) {
            for (double i : (double[]) object) {
                bsonWriter.writeDouble(i);
            }
        } else if (object instanceof boolean[]) {
            for (boolean i : (boolean[]) object) {
                bsonWriter.writeBoolean(i);
            }
        } else if (object instanceof String[]) {
            for (String i : (String[]) object) {
                bsonWriter.writeString(i);
            }
        } else {
            final int length = Array.getLength(object);
            for (int i = 0; i < length; i++) {
                putObjectField(String.valueOf(i), Array.get(object, i));
            }
        }
        bsonWriter.writeEndArray();
    }

    @SuppressWarnings("rawtypes")
    protected void putIterable(final String name, final Iterable iterable) {
        putName(name);
        bsonWriter.writeStartArray();
        final int i = 0;
        for (Object o : iterable) {
            putObjectField(String.valueOf(i), o);
        }
        bsonWriter.writeEndArray();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected void putMap(final String name, final Map map) {
        putName(name);
        bsonWriter.writeStartDocument();
        for (Map.Entry entry : (Set<Map.Entry>) map.entrySet()) {
            putObjectField((String) entry.getKey(), entry.getValue());
        }
        bsonWriter.writeEndDocument();
    }

    protected int putObject(final String name, final BSONObject document) {
        putName(name);
        return putObject(document);
    }

    protected boolean putSpecial(final String name, final Object special) {
        return false;
    }

    protected void putMinKey(final String name) {
        putName(name);
        bsonWriter.writeMinKey();
    }

    protected void putMaxKey(final String name) {
        putName(name);
        bsonWriter.writeMaxKey();
    }

    private static void writeLongToArrayLittleEndian(final byte[] bytes, final int offset, final long x) {
        bytes[offset] = (byte) (0xFFL & (x));
        bytes[offset + 1] = (byte) (0xFFL & (x >> 8));
        bytes[offset + 2] = (byte) (0xFFL & (x >> 16));
        bytes[offset + 3] = (byte) (0xFFL & (x >> 24));
        bytes[offset + 4] = (byte) (0xFFL & (x >> 32));
        bytes[offset + 5] = (byte) (0xFFL & (x >> 40));
        bytes[offset + 6] = (byte) (0xFFL & (x >> 48));
        bytes[offset + 7] = (byte) (0xFFL & (x >> 56));
    }

}
