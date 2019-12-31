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

package org.bson.codecs

import org.bson.BsonBinaryReader
import org.bson.BsonBinaryWriter
import org.bson.BsonDocument
import org.bson.BsonDocumentWriter
import org.bson.ByteBufNIO
import org.bson.UuidRepresentation
import org.bson.codecs.configuration.CodecConfigurationException
import org.bson.io.BasicOutputBuffer
import org.bson.io.ByteBufferBsonInput
import spock.lang.Shared
import spock.lang.Specification

import java.nio.ByteBuffer

/**
 *
 */
class UuidCodecSpecification extends Specification {

    @Shared private UuidCodec uuidCodec
    @Shared private BasicOutputBuffer outputBuffer

    def setup() {
        uuidCodec = new UuidCodec()
        outputBuffer = new BasicOutputBuffer()
    }

    def 'should default to unspecified representation'() {
        expect:
        new UuidCodec().getUuidRepresentation() == UuidRepresentation.UNSPECIFIED
    }

    def 'should decode different types of UUID'(UuidCodec codec, byte[] list) throws IOException {
        given:

        ByteBufferBsonInput inputBuffer = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(list)))
        BsonBinaryReader bsonReader = new BsonBinaryReader(inputBuffer)
        UUID expectedUuid = UUID.fromString('08070605-0403-0201-100f-0e0d0c0b0a09')

        bsonReader.readStartDocument()
        bsonReader.readName()

        when:
        UUID actualUuid = codec.decode(bsonReader, DecoderContext.builder().build())

        then:
        expectedUuid == actualUuid

        cleanup:
        bsonReader.close()

        where:

        codec << [
                new UuidCodec(UuidRepresentation.JAVA_LEGACY),
                new UuidCodec(UuidRepresentation.STANDARD),
                new UuidCodec(UuidRepresentation.PYTHON_LEGACY),
                new UuidCodec(UuidRepresentation.C_SHARP_LEGACY),
        ]

        list << [
                [0, 0, 0, 0,       //Start of document
                 5,                // type (BINARY)
                 95, 105, 100, 0,  // "_id"
                 16, 0, 0, 0,      // int "16" (length)
                 3,                // type (B_UUID_LEGACY) JAVA_LEGACY
                 1, 2, 3, 4, 5, 6, 7, 8,
                 9, 10, 11, 12, 13, 14, 15, 16], //8 bytes for long, 2 longs for UUID, Little Endian

                [0, 0, 0, 0,       //Start of document
                 5,                // type (BINARY)
                 95, 105, 100, 0,  // "_id"
                 16, 0, 0, 0,      // int "16" (length)
                 4,                // type (UUID)
                 8, 7, 6, 5, 4, 3, 2, 1,
                 16, 15, 14, 13, 12, 11, 10, 9], //8 bytes for long, 2 longs for UUID, Big Endian

                [0, 0, 0, 0,       //Start of document
                 5,                // type (BINARY)
                 95, 105, 100, 0,  // "_id"
                 16, 0, 0, 0,      // int "16" (length)
                 3,                // type (B_UUID_LEGACY) PYTHON_LEGACY
                 8, 7, 6, 5, 4, 3, 2, 1,
                 16, 15, 14, 13, 12, 11, 10, 9], //8 bytes for long, 2 longs for UUID, Big Endian

                [0, 0, 0, 0,       //Start of document
                 5,                // type (BINARY)
                 95, 105, 100, 0,  // "_id"
                 16, 0, 0, 0,      // int "16" (length)
                 3,                // type (B_UUID_LEGACY) CSHARP_LEGACY
                 5, 6, 7, 8, 3, 4, 1, 2,
                 16, 15, 14, 13, 12, 11, 10, 9], //8 bytes for long, 2 longs for UUID, Big Endian
        ]
    }

    def 'should encode different types of UUIDs'(Byte bsonSubType,
                                                 UuidCodec codec,
                                                 UUID uuid) throws IOException {
        given:

        byte[] encodedDoc = [0, 0, 0, 0,       //Start of document
                             5,                // type (BINARY)
                             95, 105, 100, 0,  // "_id"
                             16, 0, 0, 0,      // int "16" (length)
                             0,                // bsonSubType
                             1, 2, 3, 4, 5, 6, 7, 8,
                             9, 10, 11, 12, 13, 14, 15, 16] //8 bytes for long, 2 longs for UUID

        encodedDoc[13] = bsonSubType

        BsonBinaryWriter bsonWriter = new BsonBinaryWriter(outputBuffer)
        bsonWriter.writeStartDocument()
        bsonWriter.writeName('_id')

        when:
        codec.encode(bsonWriter, uuid, EncoderContext.builder().build())

        then:
        outputBuffer.toByteArray() == encodedDoc

        cleanup:
        bsonWriter.close()

        where:

        bsonSubType << [3, 4, 3, 3]

        codec << [
                new UuidCodec(UuidRepresentation.JAVA_LEGACY),
                new UuidCodec(UuidRepresentation.STANDARD),
                new UuidCodec(UuidRepresentation.PYTHON_LEGACY),
                new UuidCodec(UuidRepresentation.C_SHARP_LEGACY),
        ]

        uuid << [
                UUID.fromString('08070605-0403-0201-100f-0e0d0c0b0a09'), // Java legacy UUID
                UUID.fromString('01020304-0506-0708-090a-0b0c0d0e0f10'), // simulated standard UUID
                UUID.fromString('01020304-0506-0708-090a-0b0c0d0e0f10'), // simulated Python UUID
                UUID.fromString('04030201-0605-0807-090a-0b0c0d0e0f10') // simulated C# UUID
        ]
    }

    def 'should throw if representation is unspecified'() {
        given:
        def codec = new UuidCodec(UuidRepresentation.UNSPECIFIED)

        when:
        codec.encode(new BsonDocumentWriter(new BsonDocument()), UUID.randomUUID(), EncoderContext.builder().build())

        then:
        thrown(CodecConfigurationException)
    }
}
