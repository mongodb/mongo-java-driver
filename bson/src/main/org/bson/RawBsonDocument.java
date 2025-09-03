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

import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.RawBsonDocumentCodec;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;
import org.bson.json.JsonMode;
import org.bson.json.JsonReader;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.bson.assertions.Assertions.isTrueArgument;
import static org.bson.assertions.Assertions.notNull;

/**
 * An immutable BSON document that is represented using only the raw bytes.
 *
 * @since 3.0
 */
public final class RawBsonDocument extends BsonDocument {
    private static final long serialVersionUID = 1L;
    private static final int MIN_BSON_DOCUMENT_SIZE = 5;

    /**
     * The raw bytes.
     */
    private final byte[] bytes;

    /**
     * The offset into bytes, which must be less than {@code bytes.length}.
     */
    private final int offset;

    /**
     * The length, which must be less than {@code offset + bytes.length}.
     */
    private final int length;

    /**
     * Parses a string in MongoDB Extended JSON format to a {@code RawBsonDocument}
     *
     * @param json the JSON string
     * @return a corresponding {@code RawBsonDocument} object
     * @see org.bson.json.JsonReader
     * @mongodb.driver.manual reference/mongodb-extended-json/ MongoDB Extended JSON
     * @since 3.3
     */
    public static RawBsonDocument parse(final String json) {
        notNull("json", json);
        return new RawBsonDocumentCodec().decode(new JsonReader(json), DecoderContext.builder().build());
    }

    /**
     * Constructs a new instance with the given byte array.  Note that it does not make a copy of the array, so do not modify it after
     * passing it to this constructor.
     *
     * @param bytes the bytes representing a BSON document.  Note that the byte array is NOT copied, so care must be taken not to modify it
     *              after passing it to this construction, unless of course that is your intention.
     */
    public RawBsonDocument(final byte[] bytes) {
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
     * @since 3.3
     */
    public RawBsonDocument(final byte[] bytes, final int offset, final int length) {
        notNull("bytes", bytes);
        isTrueArgument("offset >= 0", offset >= 0);
        isTrueArgument("offset < bytes.length", offset < bytes.length);
        isTrueArgument("length <= bytes.length - offset", length <= bytes.length - offset);
        isTrueArgument("length >= 5", length >= MIN_BSON_DOCUMENT_SIZE);
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
    }

    /**
     * Construct a new instance from the given document and codec for the document type.
     *
     * @param document the document to transform
     * @param codec    the codec to facilitate the transformation
     * @param <T>      the BSON type that the codec encodes/decodes
     */
    public <T> RawBsonDocument(final T document, final Codec<T> codec) {
        notNull("document", document);
        notNull("codec", codec);
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        try (BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
            codec.encode(writer, document, EncoderContext.builder().build());
            this.bytes = buffer.getInternalBuffer();
            this.offset = 0;
            this.length = buffer.getPosition();
        }
    }

    /**
     * Returns a {@code ByteBuf} that wraps the byte array, with the proper byte order.  Any changes made to the returned will be reflected
     * in the underlying byte array owned by this instance.
     *
     * @return a byte buffer that wraps the byte array owned by this instance.
     */
    public ByteBuf getByteBuffer() {
        ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, length);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        return new ByteBufNIO(buffer);
    }

    /**
     * Decode this into a document.
     *
     * @param codec the codec to facilitate the transformation
     * @param <T>   the BSON type that the codec encodes/decodes
     * @return the decoded document
     */
    public <T> T decode(final Codec<T> codec) {
        return decode((Decoder<T>) codec);
    }

    /**
     * Decode this into a document.
     *
     * @param decoder the decoder to facilitate the transformation
     * @param <T>   the BSON type that the codec encodes/decodes
     * @return the decoded document
     * @since 3.6
     */
    public <T> T decode(final Decoder<T> decoder) {
        try (BsonBinaryReader reader = createReader()) {
            return decoder.decode(reader, DecoderContext.builder().build());
        }
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
        try (BsonBinaryReader bsonReader = createReader()) {
            bsonReader.readStartDocument();
            if (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                return false;
            }
            bsonReader.readEndDocument();
        }

        return true;
    }

    @Override
    public int size() {
        int size = 0;
        try (BsonBinaryReader bsonReader = createReader()) {
            bsonReader.readStartDocument();
            while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                size++;
                bsonReader.readName();
                bsonReader.skipValue();
            }
            bsonReader.readEndDocument();
        }

        return size;
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
    public String getFirstKey() {
        try (BsonBinaryReader bsonReader = createReader()) {
            bsonReader.readStartDocument();
            try {
                return bsonReader.readName();
            } catch (BsonInvalidOperationException e) {
                throw new NoSuchElementException();
            }
        }
    }

    @Override
    public boolean containsKey(final Object key) {
        if (key == null) {
            throw new IllegalArgumentException("key can not be null");
        }

        try (BsonBinaryReader bsonReader = createReader()) {
            bsonReader.readStartDocument();
            while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                if (bsonReader.readName().equals(key)) {
                    return true;
                }
                bsonReader.skipValue();
            }
            bsonReader.readEndDocument();
        }

        return false;
    }

    @Override
    public boolean containsValue(final Object value) {
        try (BsonBinaryReader bsonReader = createReader()) {
            bsonReader.readStartDocument();
            while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                bsonReader.skipName();
                if (RawBsonValueHelper.decode(bytes, bsonReader).equals(value)) {
                    return true;
                }
            }
            bsonReader.readEndDocument();
        }

        return false;
    }

    @Override
    public BsonValue get(final Object key) {
        notNull("key", key);

        try (BsonBinaryReader bsonReader = createReader()) {
            bsonReader.readStartDocument();
            while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                if (bsonReader.readName().equals(key)) {
                    return RawBsonValueHelper.decode(bytes, bsonReader);
                }
                bsonReader.skipValue();
            }
            bsonReader.readEndDocument();
        }

        return null;
    }

    @Override
    public String toJson() {
        return toJson(JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build());
    }

    @Override
    public String toJson(final JsonWriterSettings settings) {
        StringWriter writer = new StringWriter();
        new RawBsonDocumentCodec().encode(new JsonWriter(writer, settings), this, EncoderContext.builder().build());
        return writer.toString();
    }

    @Override
    public boolean equals(final Object o) {
        return toBaseBsonDocument().equals(o);
    }

    @Override
    public int hashCode() {
        return toBaseBsonDocument().hashCode();
    }

    @Override
    public BsonDocument clone() {
        return new RawBsonDocument(bytes.clone(), offset, length);
    }

    private BsonBinaryReader createReader() {
        return new BsonBinaryReader(new ByteBufferBsonInput(getByteBuffer()));
    }

    // Transform to an org.bson.BsonDocument instance
    private BsonDocument toBaseBsonDocument() {
        try (BsonBinaryReader bsonReader = createReader()) {
            return new BsonDocumentCodec().decode(bsonReader, DecoderContext.builder().build());
        }
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
        return new SerializationProxy(this.bytes, offset, length);
    }

    /**
     * Prevent normal deserialization.
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
            return new RawBsonDocument(bytes);
        }
    }
}
