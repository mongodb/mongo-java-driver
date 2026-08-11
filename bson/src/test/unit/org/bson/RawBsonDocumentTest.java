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
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RawBsonDocumentTest {

    private static final BsonDocument DOCUMENT = new BsonDocument()
            .append("a", new BsonInt32(1))
            .append("b", new BsonInt32(2))
            .append("c", new BsonDocument("x", BsonBoolean.TRUE))
            .append("d", new BsonArray(Arrays.asList(
                    new BsonDocument("y", BsonBoolean.FALSE),
                    new BsonArray(Arrays.asList(new BsonInt32(1))))));

    private static final byte[] DOCUMENT_BYTES = encodeDocument();

    static Stream<Arguments> backingArrayAccessors() {
        int documentLength = DOCUMENT_BYTES.length;

        Stream.Builder<Arguments> builder = Stream.builder();
        builder.add(Arguments.of(createFromDocument(), 0, documentLength));
        builder.add(Arguments.of(createFromByteArray(), 0, documentLength));

        for (int padding = 1; padding <= 2; padding++) {
            builder.add(Arguments.of(createPaddedBefore(padding), padding, documentLength));
            builder.add(Arguments.of(createPaddedAfter(padding), 0, documentLength));
            builder.add(Arguments.of(createPaddedBoth(padding), padding, documentLength));
        }

        return builder.build();
    }

    @ParameterizedTest(name = "{0}, expectedOffset={1}, expectedLength={2}")
    @MethodSource("backingArrayAccessors")
    void shouldExposeBackingArrayOffsetAndLength(final RawBsonDocument rawDocument,
                                                 final int expectedOffset,
                                                 final int expectedLength) {
        assertEquals(expectedOffset, rawDocument.getByteOffset());
        assertEquals(expectedLength, rawDocument.getByteLength());
        assertArrayEquals(DOCUMENT_BYTES,
                Arrays.copyOfRange(
                        rawDocument.getBackingArray(),
                        rawDocument.getByteOffset(),
                        rawDocument.getByteOffset() + rawDocument.getByteLength()));
    }

    private static Named<RawBsonDocument> createFromDocument() {
        return Named.of("from document", new RawBsonDocument(DOCUMENT, new BsonDocumentCodec()));
    }

    private static Named<RawBsonDocument> createFromByteArray() {
        return Named.of("from byte array", new RawBsonDocument(DOCUMENT_BYTES));
    }

    private static Named<RawBsonDocument> createPaddedBefore(final int padding) {
        byte[] padded = new byte[DOCUMENT_BYTES.length + padding];
        System.arraycopy(DOCUMENT_BYTES, 0, padded, padding, DOCUMENT_BYTES.length);
        return Named.of("padded before " + padding, new RawBsonDocument(padded, padding, DOCUMENT_BYTES.length));
    }

    private static Named<RawBsonDocument> createPaddedAfter(final int padding) {
        byte[] padded = new byte[DOCUMENT_BYTES.length + padding];
        System.arraycopy(DOCUMENT_BYTES, 0, padded, 0, DOCUMENT_BYTES.length);
        return Named.of("padded after " + padding, new RawBsonDocument(padded, 0, DOCUMENT_BYTES.length));
    }

    private static Named<RawBsonDocument> createPaddedBoth(final int padding) {
        byte[] padded = new byte[DOCUMENT_BYTES.length + padding * 2];
        System.arraycopy(DOCUMENT_BYTES, 0, padded, padding, DOCUMENT_BYTES.length);
        return Named.of("padded both " + padding, new RawBsonDocument(padded, padding, DOCUMENT_BYTES.length));
    }

    private static byte[] encodeDocument() {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        new BsonDocumentCodec().encode(new BsonBinaryWriter(buffer), DOCUMENT, EncoderContext.builder().build());
        return Arrays.copyOf(buffer.getInternalBuffer(), buffer.getPosition());
    }
}
