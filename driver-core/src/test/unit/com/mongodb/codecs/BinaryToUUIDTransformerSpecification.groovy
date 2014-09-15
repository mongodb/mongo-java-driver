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

package com.mongodb.codecs

import org.bson.BsonBinary
import org.bson.BsonBinaryReader
import org.bson.ByteBufNIO
import org.bson.io.ByteBufferBsonInputStream
import spock.lang.Specification
import spock.lang.Subject

import static java.nio.ByteBuffer.wrap

class BinaryToUUIDTransformerSpecification extends Specification {

    @Subject
    private final BinaryToUUIDTransformer binaryToUUIDTransformer = new BinaryToUUIDTransformer();

    def 'should read little endian encoded longs'() {
        given:
        byte[] binaryTypeWithUUIDAsBytes = [
                0, 0, 0, 0,            // document
                5,                      // type (BINARY)
                95, 105, 100, 0,        // "_id"
                16, 0, 0, 0,            // int "16" (length)
                4,                      // type (B_UUID_STANDARD)
                2, 0, 0, 0, 0, 0, 0, 0, //
                1, 0, 0, 0, 0, 0, 0, 0, // 8 bytes for long, 2 longs for UUID
                0];                     // EOM
        BsonBinaryReader reader =
                new BsonBinaryReader(new ByteBufferBsonInputStream(new ByteBufNIO(wrap(binaryTypeWithUUIDAsBytes))), true);
        BsonBinary binary;
        try {
            reader.readStartDocument();
            binary = reader.readBinaryData();
        } finally {
            reader.close();
        }

        when:
        UUID actualUUID = binaryToUUIDTransformer.transform(binary);

        then:
        actualUUID == new UUID(2L, 1L);
    }
}
