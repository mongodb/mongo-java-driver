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
import org.bson.BsonDbPointer
import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.BsonInt32
import org.bson.BsonReader
import org.bson.BsonRegularExpression
import org.bson.BsonTimestamp
import org.bson.BsonUndefined
import org.bson.BsonWriter
import org.bson.ByteBufNIO
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistry
import org.bson.io.BasicOutputBuffer
import org.bson.io.ByteBufferBsonInput
import org.bson.json.JsonReader
import org.bson.types.Binary
import org.bson.types.Code
import org.bson.types.CodeWithScope
import org.bson.types.MaxKey
import org.bson.types.MinKey
import org.bson.types.ObjectId
import org.bson.types.Symbol
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

import static java.util.Arrays.asList
import static org.bson.UuidRepresentation.C_SHARP_LEGACY
import static org.bson.UuidRepresentation.JAVA_LEGACY
import static org.bson.UuidRepresentation.PYTHON_LEGACY
import static org.bson.UuidRepresentation.STANDARD
import static org.bson.UuidRepresentation.UNSPECIFIED
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries

class DocumentCodecSpecification extends Specification {
    static final CodecRegistry REGISTRY = fromRegistries(fromCodecs(new UuidCodec(STANDARD)),
            fromProviders(asList(new ValueCodecProvider(), new IterableCodecProvider(),
                    new BsonValueCodecProvider(), new DocumentCodecProvider(), new MapCodecProvider())))

    @Shared
    BsonDocument bsonDoc = new BsonDocument()
    @Shared
    StringWriter stringWriter = new StringWriter()

    def 'should encode and decode all default types with all readers and writers'(BsonWriter writer) {
        given:
        def originalDocument = new Document()
        originalDocument.with {
            put('null', null)
            put('int32', 42)
            put('int64', 52L)
            put('booleanTrue', true)
            put('booleanFalse', false)
            put('date', new Date())
            put('dbPointer', new BsonDbPointer('foo.bar', new ObjectId()))
            put('double', 62.0 as double)
            put('minKey', new MinKey())
            put('maxKey', new MaxKey())
            put('code', new Code('int i = 0;'))
            put('codeWithScope', new CodeWithScope('int x = y', new Document('y', 1)))
            put('objectId', new ObjectId())
            put('regex', new BsonRegularExpression('^test.*regex.*xyz$', 'i'))
            put('string', 'the fox ...')
            put('symbol', new Symbol('ruby stuff'))
            put('timestamp', new BsonTimestamp(0x12345678, 5))
            put('undefined', new BsonUndefined())
            put('binary', new Binary((byte) 0x80, [5, 4, 3, 2, 1] as byte[]))
            put('array', asList(1, 1L, true, [1, 2, 3], new Document('a', 1), null))
            put('uuid', new UUID(1L, 2L))
            put('document', new Document('a', 2))
            put('map', [a:1, b:2])
            put('atomicLong', new AtomicLong(1))
            put('atomicInteger', new AtomicInteger(1))
            put('atomicBoolean', new AtomicBoolean(true))
        }

        when:
        new DocumentCodec(REGISTRY).withUuidRepresentation(STANDARD)
                .encode(writer, originalDocument, EncoderContext.builder().build())
        BsonReader reader
        if (writer instanceof BsonDocumentWriter) {
            reader = new BsonDocumentReader(bsonDoc)
        } else if (writer instanceof BsonBinaryWriter) {
            BasicOutputBuffer buffer = (BasicOutputBuffer)writer.getBsonOutput()
            reader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(
                    ByteBuffer.wrap(buffer.toByteArray()))))
        } else {
            reader = new JsonReader(stringWriter.toString())
        }
        def decodedDoc = new DocumentCodec(REGISTRY).withUuidRepresentation(STANDARD).decode(reader, DecoderContext.builder().build())

        then:
        decodedDoc.get('null') == originalDocument.get('null')
        decodedDoc.get('int32') == originalDocument.get('int32')
        decodedDoc.get('int64') == originalDocument.get('int64')
        decodedDoc.get('booleanTrue') == originalDocument.get('booleanTrue')
        decodedDoc.get('booleanFalse') == originalDocument.get('booleanFalse')
        decodedDoc.get('date') == originalDocument.get('date')
        decodedDoc.get('dbPointer') == originalDocument.get('dbPointer')
        decodedDoc.get('double') == originalDocument.get('double')
        decodedDoc.get('minKey') == originalDocument.get('minKey')
        decodedDoc.get('maxKey') == originalDocument.get('maxKey')
        decodedDoc.get('code') == originalDocument.get('code')
        decodedDoc.get('codeWithScope') == originalDocument.get('codeWithScope')
        decodedDoc.get('objectId') == originalDocument.get('objectId')
        decodedDoc.get('regex') == originalDocument.get('regex')
        decodedDoc.get('string') == originalDocument.get('string')
        decodedDoc.get('symbol') == originalDocument.get('symbol')
        decodedDoc.get('timestamp') == originalDocument.get('timestamp')
        decodedDoc.get('undefined') == originalDocument.get('undefined')
        decodedDoc.get('binary') == originalDocument.get('binary')
        decodedDoc.get('uuid') == originalDocument.get('uuid')
        decodedDoc.get('array') == originalDocument.get('array')
        decodedDoc.get('document') == originalDocument.get('document')
        decodedDoc.get('map') == originalDocument.get('map')
        decodedDoc.get('atomicLong')  == ((AtomicLong) originalDocument.get('atomicLong')).get()
        decodedDoc.get('atomicInteger')  == ((AtomicInteger) originalDocument.get('atomicInteger')).get()
        decodedDoc.get('atomicBoolean') == ((AtomicBoolean) originalDocument.get('atomicBoolean')).get()

        where:
        writer << [
                new BsonDocumentWriter(bsonDoc),
                new BsonBinaryWriter(new BasicOutputBuffer()),
//                new JsonWriter(stringWriter)
        ]
    }

    def 'should decode binary subtypes for UUID that are not 16 bytes into Binary'() {
        given:
        def reader = new BsonBinaryReader(ByteBuffer.wrap(bytes as byte[]))

        when:
        def document = new DocumentCodec().decode(reader, DecoderContext.builder().build())

        then:
        value == document.get('f')

        where:
        value                                            | bytes
        new Binary((byte) 0x03, (byte[]) [115, 116, 11]) | [16, 0, 0, 0, 5, 102, 0, 3, 0, 0, 0, 3, 115, 116, 11, 0]
        new Binary((byte) 0x04, (byte[]) [115, 116, 11]) | [16, 0, 0, 0, 5, 102, 0, 3, 0, 0, 0, 4, 115, 116, 11, 0]
    }

    @SuppressWarnings(['LineLength'])
    @Unroll
    def 'should decode binary subtype 3 for UUID'() {
        given:
        def reader = new BsonBinaryReader(ByteBuffer.wrap(bytes as byte[]))

        when:
        def document = new DocumentCodec(fromCodecs(new UuidCodec(representation), new BinaryCodec()))
                .withUuidRepresentation(representation)
                .decode(reader, DecoderContext.builder().build())

        then:
        value == document.get('f')

        where:
        representation | value                                                   | bytes
        JAVA_LEGACY    | UUID.fromString('08070605-0403-0201-100f-0e0d0c0b0a09') | [29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]
        C_SHARP_LEGACY | UUID.fromString('04030201-0605-0807-090a-0b0c0d0e0f10') | [29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]
        PYTHON_LEGACY  | UUID.fromString('01020304-0506-0708-090a-0b0c0d0e0f10') | [29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]
        STANDARD       | new Binary((byte) 3, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] as byte[]) | [29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]
        UNSPECIFIED    | new Binary((byte) 3, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] as byte[]) | [29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]
    }

    @SuppressWarnings(['LineLength'])
    @Unroll
    def 'should decode binary subtype 4 for UUID'() {
        given:
        def reader = new BsonBinaryReader(ByteBuffer.wrap(bytes as byte[]))

        when:
        def document = new DocumentCodec(fromCodecs(new UuidCodec(representation), new BinaryCodec()))
                .withUuidRepresentation(representation)
                .decode(reader, DecoderContext.builder().build())

        then:
        value == document.get('f')

        where:
        representation | value                                                                                   | bytes
        STANDARD       | UUID.fromString('01020304-0506-0708-090a-0b0c0d0e0f10')                                 | [29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 4, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]
        JAVA_LEGACY    | new Binary((byte) 4, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] as byte[]) | [29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 4, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]
        C_SHARP_LEGACY | new Binary((byte) 4, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] as byte[]) | [29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 4, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]
        PYTHON_LEGACY  | new Binary((byte) 4, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] as byte[]) | [29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 4, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]
        UNSPECIFIED    | new Binary((byte) 4, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] as byte[]) | [29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 4, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]
    }

    def 'should respect encodeIdFirst property in encoder context'() {
        given:
        def originalDocument = new Document('x', 2)
                .append('_id', 2)
                .append('nested', new Document('x', 2).append('_id', 2))
                .append('array', asList(new Document('x', 2).append('_id', 2)))

        when:
        def encodedDocument = new BsonDocument()
        new DocumentCodec().encode(new BsonDocumentWriter(encodedDocument), originalDocument,
                                   EncoderContext.builder().isEncodingCollectibleDocument(true).build())

        then:
        encodedDocument.keySet() as List == ['_id', 'x', 'nested', 'array']
        encodedDocument.getDocument('nested').keySet() as List == ['x', '_id']
        encodedDocument.getArray('array').get(0).asDocument().keySet() as List == ['x', '_id']

        when:
        encodedDocument.clear()
        new DocumentCodec().encode(new BsonDocumentWriter(encodedDocument), originalDocument,
                                   EncoderContext.builder().isEncodingCollectibleDocument(false).build())

        then:
        encodedDocument.keySet() as List == ['x', '_id', 'nested', 'array']
        encodedDocument.getDocument('nested').keySet() as List == ['x', '_id']
        encodedDocument.getArray('array').get(0).asDocument().keySet() as List == ['x', '_id']
    }

    def 'should apply transformer to decoded values'() {
        given:
        def codec = new DocumentCodec(fromProviders([new ValueCodecProvider(), new DocumentCodecProvider(), new BsonValueCodecProvider()]),
                                      new BsonTypeClassMap(),
                                      { Object value -> 5 })
        when:
        def doc = codec.decode(new BsonDocumentReader(new BsonDocument('_id', new BsonInt32(1))), DecoderContext.builder().build())

        then:
        doc['_id'] == 5
    }


    def 'should generate id if absent'() {
        given:
        def document = new Document()

        when:
        document = new DocumentCodec().generateIdIfAbsentFromDocument(document)

        then:
        document.get('_id') instanceof ObjectId
    }

    def 'should not generate id if present'() {
        given:
        def document = new Document('_id', 1)

        when:
        document = new DocumentCodec().generateIdIfAbsentFromDocument(document)

        then:
        document.get('_id') == 1
    }

    def 'should determine if id is present'() {
        expect:
        new DocumentCodec().documentHasId(new Document('_id', 1))
        !new DocumentCodec().documentHasId(new Document())
    }

    def 'should get id if present'() {
        expect:
        new DocumentCodec().getDocumentId(new Document('_id', 1)) == new BsonInt32(1)
        new DocumentCodec().getDocumentId(new Document('_id', new BsonInt32(1))) == new BsonInt32(1)
    }

    def 'should throw if getting id when absent'() {
        when:
        new DocumentCodec().getDocumentId(new Document())

        then:
        thrown(IllegalStateException)
    }
}
