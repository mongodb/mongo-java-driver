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

package com.mongodb.codecs;

import org.bson.BsonBinaryWriter;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class UUIDCodecTest {

    private UUIDCodec uuidCodec;
    private BasicOutputBuffer outputBuffer;

    @Before
    public void setUp() throws Exception {
        uuidCodec = new UUIDCodec();
        outputBuffer = new BasicOutputBuffer();
    }

    @Test
    public void shouldEncodeLongAsLittleEndian() throws IOException {
        // Given
        UUID uuid = new UUID(2L, 1L);
        BsonBinaryWriter bsonWriter = new BsonBinaryWriter(outputBuffer, false);
        try {
            bsonWriter.writeStartDocument();
            bsonWriter.writeName("_id");

            // When
            uuidCodec.encode(bsonWriter, uuid, EncoderContext.builder().build());
        } finally {
            bsonWriter.close();
        }

        // Then
        byte[] expectedList = {0, 0, 0, 0,       //Start of document
                               5,                // type (BINARY)
                               95, 105, 100, 0,  // "_id"
                               16, 0, 0, 0,      // int "16" (length)
                               3,                // type (B_UUID_LEGACY)
                               2, 0, 0, 0, 0, 0, 0, 0,
                               1, 0, 0, 0, 0, 0, 0, 0}; //8 bytes for long, 2 longs for UUID, Little Endian

        assertThat(outputBuffer.toByteArray(), is(expectedList));
    }

}
