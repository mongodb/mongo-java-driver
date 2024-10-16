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

import org.bson.BSONException;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.Vector;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.OutputBuffer;
import org.bson.types.Binary;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class VectorCodecTest extends CodecTestCase {

    private static final CodecRegistry CODEC_REGISTRIES = fromProviders(asList(new ValueCodecProvider(), new DocumentCodecProvider()));

    private static Stream<Arguments> provideVectorsAndCodecsForRoundTrip() {
        return Stream.of(
                arguments(Vector.floatVector(new float[]{1.1f, 2.2f, 3.3f}), new Float32VectorCodec()),
                arguments(Vector.int8Vector(new byte[]{10, 20, 30, 40}), new Int8VectorCodec()),
                arguments(Vector.packedBitVector(new byte[]{(byte) 0b10101010, (byte) 0b01010101}, (byte) 3), new PackedBitVectorCodec())
        );
    }

    @ParameterizedTest
    @MethodSource("provideVectorsAndCodecsForRoundTrip")
    void shouldRoundTripVectors(final Vector vectorToEncode) {
        //given
        Document expectedDocument = new Document("vector", vectorToEncode);

        //when
        Codec<Document> codec = CODEC_REGISTRIES.get(Document.class);
        OutputBuffer buffer = encode(codec, expectedDocument);
        Document actualDecodedDocument = decode(codec, buffer);

        //then
        Binary binaryVector = (Binary) actualDecodedDocument.get("vector");
        assertNotEquals(actualDecodedDocument, expectedDocument);
        Vector actualVector = binaryVector.asVector();
        assertEquals(actualVector, vectorToEncode);
    }

    @ParameterizedTest
    @MethodSource("provideVectorsAndCodecsForRoundTrip")
    void shouldEncodeVector(final Vector vectorToEncode, final Codec<Vector> vectorCodec) {
        // given
        BsonWriter mockWriter = Mockito.mock(BsonWriter.class);

        // when
        vectorCodec.encode(mockWriter, vectorToEncode, EncoderContext.builder().build());

        // then
        verify(mockWriter, times(1)).writeBinaryData(new BsonBinary(vectorToEncode));
        verifyNoMoreInteractions(mockWriter);
    }

    @ParameterizedTest
    @MethodSource("provideVectorsAndCodecsForRoundTrip")
    void shouldDecodeVector(final Vector vectorToDecode, final Codec<Vector> vectorCodec) {
        // given
        BsonReader mockReader = Mockito.mock(BsonReader.class);
        BsonBinary bsonBinary = new BsonBinary(vectorToDecode);
        when(mockReader.peekBinarySubType()).thenReturn(BsonBinarySubType.VECTOR.getValue());
        when(mockReader.readBinaryData()).thenReturn(bsonBinary);

        // when
        Vector decodedVector = vectorCodec.decode(mockReader, DecoderContext.builder().build());

        // then
        assertNotNull(decodedVector);
        assertEquals(vectorToDecode, decodedVector);
    }


    @ParameterizedTest
    @EnumSource(value = BsonBinarySubType.class, mode = EnumSource.Mode.EXCLUDE, names = {"VECTOR"})
    void shouldThrowExceptionForInvalidSubType(final BsonBinarySubType subType) {
        // given
        BsonReader mockReader = Mockito.mock(BsonReader.class);
        when(mockReader.peekBinarySubType()).thenReturn(subType.getValue());

        Stream.of(new Float32VectorCodec(), new Int8VectorCodec(), new PackedBitVectorCodec())
                .forEach(codec -> {
                    // when & then
                    BSONException exception = assertThrows(BSONException.class, () ->
                            codec.decode(mockReader, DecoderContext.builder().build()));
                    assertEquals("Unexpected BsonBinarySubType", exception.getMessage());
                });
    }


    @ParameterizedTest
    @MethodSource("provideVectorsAndCodecsForRoundTrip")
    void shouldReturnCorrectEncoderClass(final Vector vector, final Codec<? extends Vector> codec) {
        // when
        Class<? extends Vector> encoderClass = codec.getEncoderClass();

        // then
        assertEquals(vector.getClass(), encoderClass);
    }

    @ParameterizedTest
    @MethodSource("provideVectorsCodec")
    void shouldConvertToString(final Codec<Vector> codec) {
        // when
        String result = codec.toString();

        // then
        assertEquals(codec.getClass().getSimpleName() + "{}", result);
    }

    private static Stream<Codec<? extends Vector>> provideVectorsCodec() {
        return Stream.of(
                new Float32VectorCodec(),
                new Int8VectorCodec(),
                new PackedBitVectorCodec()
        );
    }
}
