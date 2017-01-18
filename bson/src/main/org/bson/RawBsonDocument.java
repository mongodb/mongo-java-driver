/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.RawBsonDocumentCodec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;
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
import java.util.Set;

import static org.bson.assertions.Assertions.isTrueArgument;
import static org.bson.assertions.Assertions.notNull;
import static org.bson.codecs.BsonValueCodecProvider.getClassForBsonType;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

/**
 * An immutable BSON document that is represented using only the raw bytes.
 *
 * @since 3.0
 */
public final class RawBsonDocument extends BsonDocument {
    private static final long serialVersionUID = 1L;
    private static final int MIN_BSON_DOCUMENT_SIZE = 5;

    private static final CodecRegistry REGISTRY = fromProviders(new BsonValueCodecProvider());

    private final byte[] bytes;
    private final int offset;
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
        BsonBinaryWriter writer = new BsonBinaryWriter(buffer);
        try {
            codec.encode(writer, document, EncoderContext.builder().build());
            this.bytes = buffer.getInternalBuffer();
            this.offset = 0;
            this.length = buffer.getPosition();
        } finally {
            writer.close();
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
        BsonBinaryReader reader = createReader();
        try {
            return codec.decode(reader, DecoderContext.builder().build());
        } finally {
            reader.close();
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
        BsonBinaryReader bsonReader = createReader();
        try {
            bsonReader.readStartDocument();
            if (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                return false;
            }
            bsonReader.readEndDocument();
        } finally {
            bsonReader.close();
        }

        return true;
    }

    @Override
    public int size() {
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

        return size;
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

        BsonBinaryReader bsonReader = createReader();
        try {
            bsonReader.readStartDocument();
            while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                if (bsonReader.readName().equals(key)) {
                    return true;
                }
                bsonReader.skipValue();
            }
            bsonReader.readEndDocument();
        } finally {
            bsonReader.close();
        }

        return false;
    }

    @Override
    public boolean containsValue(final Object value) {
        BsonBinaryReader bsonReader = createReader();
        try {
            bsonReader.readStartDocument();
            while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                bsonReader.skipName();
                if (deserializeBsonValue(bsonReader).equals(value)) {
                    return true;
                }
            }
            bsonReader.readEndDocument();
        } finally {
            bsonReader.close();
        }

        return false;
    }

    @Override
    public BsonValue get(final Object key) {
        notNull("key", key);

        BsonBinaryReader bsonReader = createReader();
        try {
            bsonReader.readStartDocument();
            while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                if (bsonReader.readName().equals(key)) {
                    return deserializeBsonValue(bsonReader);
                }
                bsonReader.skipValue();
            }
            bsonReader.readEndDocument();
        } finally {
            bsonReader.close();
        }

        return null;
    }

    @Override
    public String toJson() {
        return toJson(new JsonWriterSettings());
    }

    @Override
    public String toJson(final JsonWriterSettings settings) {
        StringWriter writer = new StringWriter();
        new RawBsonDocumentCodec().encode(new JsonWriter(writer, settings), this, EncoderContext.builder().build());
        return writer.toString();
    }

    @Override
    public boolean equals(final Object o) {
        return toBsonDocument().equals(o);
    }

    @Override
    public int hashCode() {
        return toBsonDocument().hashCode();
    }

    @Override
    public BsonDocument clone() {
        return new RawBsonDocument(bytes.clone(), offset, length);
    }

    private BsonValue deserializeBsonValue(final BsonBinaryReader bsonReader) {
        return REGISTRY.get(getClassForBsonType(bsonReader.getCurrentBsonType())).decode(bsonReader, DecoderContext.builder().build());
    }

    private BsonBinaryReader createReader() {
        return new BsonBinaryReader(new ByteBufferBsonInput(getByteBuffer()));
    }

    private BsonDocument toBsonDocument() {
        BsonBinaryReader bsonReader = createReader();
        try {
            return new BsonDocumentCodec().decode(bsonReader, DecoderContext.builder().build());
        } finally {
            bsonReader.close();
        }
    }

    // see https://docs.oracle.com/javase/6/docs/platform/serialization/spec/output.html
    private Object writeReplace() {
        return new SerializationProxy(this.bytes, offset, length);
    }

    // see https://docs.oracle.com/javase/6/docs/platform/serialization/spec/input.html
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
