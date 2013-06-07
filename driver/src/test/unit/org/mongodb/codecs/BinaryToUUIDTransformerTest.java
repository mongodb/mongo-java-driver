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

package org.mongodb.codecs;

import org.bson.BSONBinaryReader;
import org.bson.io.BasicInputBuffer;
import org.bson.types.Binary;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static java.nio.ByteBuffer.wrap;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class BinaryToUUIDTransformerTest {
    private BinaryToUUIDTransformer binaryToUUIDTransformer;

    @Before
    public void setUp() throws Exception {
        binaryToUUIDTransformer = new BinaryToUUIDTransformer();
    }

    @Test
    public void shouldReadLittleEndianEncodedLongs() {
        // Given
        final byte[] binaryTypeWithUUIDAsBytes = {
                0, 0, 0, 0,            // document
                5,                      // type (BINARY)
                95, 105, 100, 0,        // "_id"
                16, 0, 0, 0,            // int "16" (length)
                4,                      // type (B_UUID_STANDARD)
                2, 0, 0, 0, 0, 0, 0, 0, //
                1, 0, 0, 0, 0, 0, 0, 0, // 8 bytes for long, 2 longs for UUID
                0};                     // EOM
        final BSONBinaryReader reader = new BSONBinaryReader(new BasicInputBuffer(wrap(binaryTypeWithUUIDAsBytes)), true);
        Binary binary;
        try {
            reader.readStartDocument();
            binary = reader.readBinaryData();
        } finally {
            reader.close();
        }

        // When
        final UUID actualUUID = binaryToUUIDTransformer.transform(binary);

        // Then
        final UUID expectedUUID = new UUID(2L, 1L);
        assertThat(actualUUID, is(expectedUUID));
    }
}
