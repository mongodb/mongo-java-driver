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

package org.bson.codecs;

import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.ByteBufNIO;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;
import org.bson.io.OutputBuffer;

import java.nio.ByteBuffer;
import java.util.HashMap;

import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.junit.Assert.assertEquals;

abstract class CodecTestCase {

    DocumentCodecProvider getDocumentCodecProvider() {
        return new DocumentCodecProvider();
    }

    CodecRegistry getRegistry() {
        return fromProviders(asList(new ValueCodecProvider(), getDocumentCodecProvider()));
    }

    <T> T getDecodedValue(BsonValue bsonValue, Decoder<T> decoder) {
        BsonDocument document = new BsonDocument("val", bsonValue);
        BsonDocumentReader reader = new BsonDocumentReader(document);
        reader.readStartDocument();
        reader.readName("val");
        return decoder.decode(reader, DecoderContext.builder().build());
    }

    <T> BsonValue getEncodedValue(T value, Encoder<T> encoder) {
        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
        writer.writeStartDocument();
        writer.writeName("val");
        encoder.encode(writer, value, EncoderContext.builder().build());
        writer.writeEndDocument();
        return writer.getDocument().get("val");
    }

    <T> void roundTrip(final T value) {
        roundTrip(value, new DefaultComparator<T>(value));
    }

    <T> void roundTrip(final T value, final Comparator<T> comparator) {
        roundTripWithRegistry(value, comparator, getRegistry());
    }

    @SuppressWarnings("unchecked")
    <T> void roundTripWithRegistry(final T value, final Comparator<T> comparator, final CodecRegistry codecRegistry) {
        Codec<T> codec = (Codec<T>) codecRegistry.get(value.getClass());
        OutputBuffer encoded = encode(codec, value);
        T decoded = decode(codec, encoded);
        comparator.apply(decoded);
    }

    public void roundTrip(final Document input, final Document expected) {
        roundTrip(input, new Comparator<Document>() {
            @Override
            public void apply(final Document result) {
                assertEquals("Codec Round Trip", expected, result);
            }
        });
    }

    <T> OutputBuffer encode(final Codec<T> codec, final T value) {
        OutputBuffer buffer = new BasicOutputBuffer();
        BsonWriter writer = new BsonBinaryWriter(buffer);
        codec.encode(writer, value, EncoderContext.builder().build());
        return buffer;
    }

    <T> T decode(final Codec<T> codec, final OutputBuffer buffer) {
        BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(buffer.toByteArray()))));
        return codec.decode(reader, DecoderContext.builder().build());
    }

    DocumentCodecProvider getSpecificNumberDocumentCodecProvider(final Class<? extends Number> clazz) {
        HashMap<BsonType, Class<?>> replacements = new HashMap<BsonType, Class<?>>();
        replacements.put(BsonType.DOUBLE, clazz);
        replacements.put(BsonType.INT32, clazz);
        replacements.put(BsonType.INT64, clazz);
        replacements.put(BsonType.DECIMAL128, clazz);
        return new DocumentCodecProvider(new BsonTypeClassMap(replacements));
    }

    interface Comparator<T> {
        void apply(T result);
    }

    class DefaultComparator<T> implements Comparator<T> {
        private final T original;

        DefaultComparator(final T original) {
            this.original = original;
        }

        @Override
        public void apply(final T result) {
            assertEquals("Codec Round Trip", original, result);
        }
    }

}
