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

import org.bson.BsonBinary;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinarySubType;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonInvalidOperationException;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.ByteBufNIO;
import org.bson.Float32BinaryVector;
import org.bson.Int8BinaryVector;
import org.bson.PackedBitBinaryVector;
import org.bson.BinaryVector;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;
import org.bson.io.OutputBuffer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static org.bson.BsonHelper.toBson;
import static org.bson.assertions.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class BinaryVectorCodecTest extends CodecTestCase {

    private static Stream<Arguments> provideVectorsAndCodecs() {
        return Stream.of(
                arguments(BinaryVector.floatVector(new float[]{1.1f, 2.2f, 3.3f}), new Float32VectorCodec(), Float32BinaryVector.class),
                arguments(BinaryVector.int8Vector(new byte[]{10, 20, 30, 40}), new Int8VectorCodec(), Int8BinaryVector.class),
                arguments(BinaryVector.packedBitVector(new byte[]{(byte) 0b10101010, (byte) 0b01010101}, (byte) 3), new PackedBitVectorCodec(), PackedBitBinaryVector.class),
                arguments(BinaryVector.packedBitVector(new byte[]{(byte) 0b10101010, (byte) 0b01010101}, (byte) 3), new VectorCodec(), BinaryVector.class),
                arguments(BinaryVector.int8Vector(new byte[]{10, 20, 30, 40}), new VectorCodec(), BinaryVector.class),
                arguments(BinaryVector.packedBitVector(new byte[]{(byte) 0b10101010, (byte) 0b01010101}, (byte) 3), new VectorCodec(), BinaryVector.class)
        );
    }

    @ParameterizedTest
    @MethodSource("provideVectorsAndCodecs")
    void shouldEncodeVector(final BinaryVector vectorToEncode, final Codec<BinaryVector> vectorCodec) throws IOException {
        // given
        BsonBinary bsonBinary = new BsonBinary(vectorToEncode);
        byte[] encodedVector = bsonBinary.getData();
        ByteArrayOutputStream expectedStream = new ByteArrayOutputStream();
        // Total length of a Document (int 32). It is 0, because we do not expect
        // codec to write the end of the document (that is when we back-patch the length of the document).
        expectedStream.write(new byte[]{0, 0, 0, 0});
        // Bson type
        expectedStream.write((byte) BsonType.BINARY.getValue());
        // Field name "b4"
        expectedStream.write(new byte[]{98, 52, 0});
        // Total length of binary data (little-endian format)
        expectedStream.write(new byte[]{(byte) encodedVector.length, 0, 0, 0});
        // Vector binary subtype
        expectedStream.write(BsonBinarySubType.VECTOR.getValue());
        // Actual BSON binary data
        expectedStream.write(encodedVector);

        OutputBuffer buffer = new BasicOutputBuffer();
        BsonWriter writer = new BsonBinaryWriter(buffer);
        writer.writeStartDocument();
        writer.writeName("b4");

        // when
        vectorCodec.encode(writer, vectorToEncode, EncoderContext.builder().build());

        // then
        assertArrayEquals(expectedStream.toByteArray(), buffer.toByteArray());
    }

    @ParameterizedTest
    @MethodSource("provideVectorsAndCodecs")
    void shouldDecodeVector(final BinaryVector vectorToDecode, final Codec<BinaryVector> vectorCodec) {
        // given
        OutputBuffer buffer = new BasicOutputBuffer();
        BsonWriter writer = new BsonBinaryWriter(buffer);
        writer.writeStartDocument();
        writer.writeName("vector");
        writer.writeBinaryData(new BsonBinary(vectorToDecode));
        writer.writeEndDocument();

        BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(buffer.toByteArray()))));
        reader.readStartDocument();

        // when
        BinaryVector decodedVector = vectorCodec.decode(reader, DecoderContext.builder().build());

        // then
        assertDoesNotThrow(reader::readEndDocument);
        assertNotNull(decodedVector);
        assertEquals(vectorToDecode, decodedVector);
    }


    @ParameterizedTest
    @EnumSource(value = BsonBinarySubType.class, mode = EnumSource.Mode.EXCLUDE, names = {"VECTOR"})
    void shouldThrowExceptionForInvalidSubType(final BsonBinarySubType subType) {
        // given
        BsonDocument document = new BsonDocument("name", new BsonBinary(subType.getValue(), new byte[]{}));
        BsonBinaryReader reader = new BsonBinaryReader(toBson(document));
        reader.readStartDocument();

        // when & then
        Stream.of(new Float32VectorCodec(), new Int8VectorCodec(), new PackedBitVectorCodec())
                .forEach(codec -> {
                    BsonInvalidOperationException exception = assertThrows(BsonInvalidOperationException.class, () ->
                            codec.decode(reader, DecoderContext.builder().build()));
                    assertEquals("Expected vector binary subtype 9 but found: " + subType.getValue(), exception.getMessage());
                });
    }


    @ParameterizedTest
    @MethodSource("provideVectorsAndCodecs")
    void shouldReturnCorrectEncoderClass(final BinaryVector vector,
                                         final Codec<? extends BinaryVector> codec,
                                         final Class<? extends BinaryVector> expectedEncoderClass) {
        // when
        Class<? extends BinaryVector> encoderClass = codec.getEncoderClass();

        // then
        assertEquals(expectedEncoderClass, encoderClass);
    }
}
