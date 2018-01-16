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

import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
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
 * This is meant to be pooled or cached. There is some per instance memory for string conversion, etc...
 */
public class BasicBSONEncoder implements BSONEncoder {

    private BsonBinaryWriter bsonWriter;
    private OutputBuffer outputBuffer;

    @Override
    public byte[] encode(final BSONObject document) {
        OutputBuffer outputBuffer = new BasicOutputBuffer();
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
        outputBuffer = buffer;
        this.bsonWriter = new BsonBinaryWriter(buffer);
    }

    /**
     * Gets the buffer the BSON is being encoded into.
     *
     * @return the OutputBuffer
     */
    protected OutputBuffer getOutputBuffer() {
        return outputBuffer;
    }

    /**
     * Gets the writer responsible for writing the encoded BSON.
     *
     * @return the writer used to write the encoded BSON
     */
    protected BsonBinaryWriter getBsonWriter() {
        return bsonWriter;
    }

    /**
     * Encodes a {@code BSONObject}. This is for the higher level api calls.
     *
     * @param document the document to encode
     * @return the number of characters in the encoding
     */
    @Override
    public int putObject(final BSONObject document) {
        int startPosition = getOutputBuffer().getPosition();
        bsonWriter.writeStartDocument();

        if (isTopLevelDocument() && document.containsField("_id")) {
            _putObjectField("_id", document.get("_id"));
        }

        for (final String key : document.keySet()) {
            if (isTopLevelDocument() && key.equals("_id")) {
                continue;
            }
            _putObjectField(key, document.get(key));
        }
        bsonWriter.writeEndDocument();
        return getOutputBuffer().getPosition() - startPosition;
    }

    private boolean isTopLevelDocument() {
        return bsonWriter.getContext().getParentContext() == null;
    }

    /**
     * Writes a field name
     *
     * @param name the field name
     */
    protected void putName(final String name) {
        if (bsonWriter.getState() == AbstractBsonWriter.State.NAME) {
            bsonWriter.writeName(name);
        }
    }

    /**
     * Encodes any Object type
     *
     * @param name         the field name
     * @param initialValue the value to write
     */
    protected void _putObjectField(final String name, final Object initialValue) {
        if ("_transientFields".equals(name)) {
            return;
        }
        if (name.contains("\0")) {
            throw new IllegalArgumentException("Document field names can't have a NULL character. (Bad Key: '" + name + "')");
        }

        if ("$where".equals(name) && initialValue instanceof String) {
            putCode(name, new Code((String) initialValue));
        }

        Object value = BSON.applyEncodingHooks(initialValue);

        if (value == null) {
            putNull(name);
        } else if (value instanceof Date) {
            putDate(name, (Date) value);
        } else if (value instanceof Number) {
            putNumber(name, (Number) value);
        } else if (value instanceof Decimal128) {
            putDecimal128(name, (Decimal128) value);
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

    /**
     * Encodes a null value
     *
     * @param name the field name
     * @see org.bson.BsonType#NULL
     */
    protected void putNull(final String name) {
        putName(name);
        bsonWriter.writeNull();
    }

    /**
     * Encodes an undefined value
     *
     * @param name the field name
     * @see org.bson.BsonType#UNDEFINED
     */
    protected void putUndefined(final String name) {
        putName(name);
        bsonWriter.writeUndefined();
    }

    /**
     * Encodes a BSON timestamp
     *
     * @param name      the field name
     * @param timestamp the timestamp to encode
     * @see org.bson.BsonType#TIMESTAMP
     */
    protected void putTimestamp(final String name, final BSONTimestamp timestamp) {
        putName(name);
        bsonWriter.writeTimestamp(new BsonTimestamp(timestamp.getTime(), timestamp.getInc()));
    }

    /**
     * Encodes a field to a {@link org.bson.BsonType#JAVASCRIPT} value.
     *
     * @param name the field name
     * @param code the value
     */
    protected void putCode(final String name, final Code code) {
        putName(name);
        bsonWriter.writeJavaScript(code.getCode());
    }

    /**
     * Encodes a field to a {@link org.bson.BsonType#JAVASCRIPT_WITH_SCOPE} value.
     *
     * @param name       the field name
     * @param codeWScope the value
     */
    protected void putCodeWScope(final String name, final CodeWScope codeWScope) {
        putName(name);
        bsonWriter.writeJavaScriptWithScope(codeWScope.getCode());
        putObject(codeWScope.getScope());
    }

    /**
     * Encodes a field with a {@code Boolean} or {@code boolean} value
     *
     * @param name  the field name
     * @param value the value
     */
    protected void putBoolean(final String name, final Boolean value) {
        putName(name);
        bsonWriter.writeBoolean(value);
    }

    /**
     * Encodes a field with data and time value.
     *
     * @param name the field name
     * @param date the value
     * @see org.bson.BsonType#DATE_TIME
     */
    protected void putDate(final String name, final Date date) {
        putName(name);
        bsonWriter.writeDateTime(date.getTime());
    }

    /**
     * Encodes any number field.
     *
     * @param name   the field name
     * @param number the value
     */
    protected void putNumber(final String name, final Number number) {
        putName(name);
        if (number instanceof Integer || number instanceof Short || number instanceof Byte || number instanceof AtomicInteger) {
            bsonWriter.writeInt32(number.intValue());
        } else if (number instanceof Long || number instanceof AtomicLong) {
            bsonWriter.writeInt64(number.longValue());
        } else if (number instanceof Float || number instanceof Double) {
            bsonWriter.writeDouble(number.doubleValue());
        } else {
            throw new IllegalArgumentException("Can't serialize " + number.getClass());
        }
    }

    /**
     * Encodes a Decimal128 field.
     *
     * @param name   the field name
     * @param value the value
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    protected void putDecimal128(final String name, final Decimal128 value) {
        putName(name);
        bsonWriter.writeDecimal128(value);
    }

    /**
     * Encodes a byte array field
     *
     * @param name  the field name
     * @param bytes the value
     * @see org.bson.BsonType#BINARY
     */
    protected void putBinary(final String name, final byte[] bytes) {
        putName(name);
        bsonWriter.writeBinaryData(new BsonBinary(bytes));
    }

    /**
     * Encodes a Binary field
     *
     * @param name   the field name
     * @param binary the value
     * @see org.bson.BsonType#BINARY
     */
    protected void putBinary(final String name, final Binary binary) {
        putName(name);
        bsonWriter.writeBinaryData(new BsonBinary(binary.getType(), binary.getData()));
    }

    /**
     * Encodes a field with a {@link java.util.UUID} value.  This is encoded to a binary value of subtype {@link
     * org.bson.BsonBinarySubType#UUID_LEGACY}
     *
     * @param name the field name
     * @param uuid the value
     */
    protected void putUUID(final String name, final UUID uuid) {
        putName(name);
        byte[] bytes = new byte[16];
        writeLongToArrayLittleEndian(bytes, 0, uuid.getMostSignificantBits());
        writeLongToArrayLittleEndian(bytes, 8, uuid.getLeastSignificantBits());
        bsonWriter.writeBinaryData(new BsonBinary(BsonBinarySubType.UUID_LEGACY, bytes));
    }

    /**
     * Encodes a Symbol field
     *
     * @param name   the field name
     * @param symbol the value
     * @see org.bson.BsonType#SYMBOL
     */
    protected void putSymbol(final String name, final Symbol symbol) {
        putName(name);
        bsonWriter.writeSymbol(symbol.getSymbol());
    }

    /**
     * Encodes a String field
     *
     * @param name  the field name
     * @param value the value
     * @see org.bson.BsonType#STRING
     */
    protected void putString(final String name, final String value) {
        putName(name);
        bsonWriter.writeString(value);
    }

    /**
     * Encodes a Pattern field to a {@link org.bson.BsonType#REGULAR_EXPRESSION}.
     *
     * @param name  the field name
     * @param value the value
     * @mongodb.driver.manual reference/operator/query/regex/ $regex
     * @see org.bson.BsonType#BINARY
     */
    protected void putPattern(final String name, final Pattern value) {
        putName(name);
        bsonWriter.writeRegularExpression(new BsonRegularExpression(value.pattern(), regexFlags(value.flags())));
    }

    /**
     * Encodes an ObjectId field to a {@link org.bson.BsonType#OBJECT_ID}.
     *
     * @param name     the field name
     * @param objectId the value
     */
    protected void putObjectId(final String name, final ObjectId objectId) {
        putName(name);
        bsonWriter.writeObjectId(objectId);
    }

    /**
     * Encodes an array field.
     *
     * @param name   the field name
     * @param object the array, which can be any sort of primitive or String array
     */
    protected void putArray(final String name, final Object object) {
        putName(name);
        bsonWriter.writeStartArray();
        if (object instanceof int[]) {
            for (final int i : (int[]) object) {
                bsonWriter.writeInt32(i);
            }
        } else if (object instanceof long[]) {
            for (final long i : (long[]) object) {
                bsonWriter.writeInt64(i);
            }
        } else if (object instanceof float[]) {
            for (final float i : (float[]) object) {
                bsonWriter.writeDouble(i);
            }
        } else if (object instanceof short[]) {
            for (final short i : (short[]) object) {
                bsonWriter.writeInt32(i);
            }
        } else if (object instanceof byte[]) {
            for (final byte i : (byte[]) object) {
                bsonWriter.writeInt32(i);
            }
        } else if (object instanceof double[]) {
            for (final double i : (double[]) object) {
                bsonWriter.writeDouble(i);
            }
        } else if (object instanceof boolean[]) {
            for (final boolean i : (boolean[]) object) {
                bsonWriter.writeBoolean(i);
            }
        } else if (object instanceof String[]) {
            for (final String i : (String[]) object) {
                bsonWriter.writeString(i);
            }
        } else {
            int length = Array.getLength(object);
            for (int i = 0; i < length; i++) {
                _putObjectField(String.valueOf(i), Array.get(object, i));
            }
        }
        bsonWriter.writeEndArray();
    }

    /**
     * Encodes an Iterable, for example {@code List} values
     *
     * @param name     the field name
     * @param iterable the value
     */
    @SuppressWarnings("rawtypes")
    protected void putIterable(final String name, final Iterable iterable) {
        putName(name);
        bsonWriter.writeStartArray();
        int i = 0;
        for (final Object o : iterable) {
            _putObjectField(String.valueOf(i), o);
        }
        bsonWriter.writeEndArray();
    }

    /**
     * Encodes a map, as a BSON document
     *
     * @param name the field name
     * @param map  the value
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected void putMap(final String name, final Map map) {
        putName(name);
        bsonWriter.writeStartDocument();
        for (final Map.Entry entry : (Set<Map.Entry>) map.entrySet()) {
            _putObjectField((String) entry.getKey(), entry.getValue());
        }
        bsonWriter.writeEndDocument();
    }

    /**
     * Encodes any {@code BSONObject}, as a document
     *
     * @param name     the field name
     * @param document the value
     * @return the number of characters in the encoding
     */
    protected int putObject(final String name, final BSONObject document) {
        putName(name);
        return putObject(document);
    }

    /**
     * Special values are not encoded into documents.
     *
     * @param name    the field name
     * @param special the value
     * @return true if the operation is successful. This implementation always returns false.
     */
    protected boolean putSpecial(final String name, final Object special) {
        return false;
    }

    /**
     * Encodes a field to a {@link org.bson.BsonType#MIN_KEY} value.
     *
     * @param name the field name
     */
    protected void putMinKey(final String name) {
        putName(name);
        bsonWriter.writeMinKey();
    }

    /**
     * Encodes a field to a {@link org.bson.BsonType#MAX_KEY} value.
     *
     * @param name the field name
     */
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
