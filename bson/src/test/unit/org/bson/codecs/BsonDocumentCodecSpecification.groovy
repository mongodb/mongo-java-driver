/*
 * Copyright (c) 2008-2016 MongoDB, Inc.
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

import org.bson.BsonArray
import org.bson.BsonBinary
import org.bson.BsonBinaryReader
import org.bson.BsonBinaryWriter
import org.bson.BsonBoolean
import org.bson.BsonDateTime
import org.bson.BsonDecimal128
import org.bson.BsonDocument
import org.bson.BsonDocumentWriter
import org.bson.BsonDouble
import org.bson.BsonElement
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonJavaScript
import org.bson.BsonJavaScriptWithScope
import org.bson.BsonMaxKey
import org.bson.BsonMinKey
import org.bson.BsonNull
import org.bson.BsonObjectId
import org.bson.BsonRegularExpression
import org.bson.BsonString
import org.bson.BsonSymbol
import org.bson.BsonTimestamp
import org.bson.BsonUndefined
import org.bson.ByteBufNIO
import org.bson.RawBsonDocument
import org.bson.io.BasicOutputBuffer
import org.bson.io.ByteBufferBsonInput
import org.bson.types.Decimal128
import org.bson.types.ObjectId
import spock.lang.Specification

import java.nio.ByteBuffer

import static java.util.Arrays.asList

class BsonDocumentCodecSpecification extends Specification {
    def 'should encode and decode all default types'() {
        given:
        def doc = new BsonDocument(
                [
                        new BsonElement('null', new BsonNull()),
                        new BsonElement('int32', new BsonInt32(42)),
                        new BsonElement('int64', new BsonInt64(52L)),
                        new BsonElement('decimal128', new BsonDecimal128(Decimal128.parse('1.0'))),
                        new BsonElement('boolean', new BsonBoolean(true)),
                        new BsonElement('date', new BsonDateTime(new Date().getTime())),
                        new BsonElement('double', new BsonDouble(62.0)),
                        new BsonElement('string', new BsonString('the fox ...')),
                        new BsonElement('minKey', new BsonMinKey()),
                        new BsonElement('maxKey', new BsonMaxKey()),
                        new BsonElement('javaScript', new BsonJavaScript('int i = 0;')),
                        new BsonElement('objectId', new BsonObjectId(new ObjectId())),
                        new BsonElement('codeWithScope', new BsonJavaScriptWithScope('int x = y', new BsonDocument('y', new BsonInt32(1)))),
                        new BsonElement('regex', new BsonRegularExpression('^test.*regex.*xyz$', 'i')),
                        new BsonElement('symbol', new BsonSymbol('ruby stuff')),
                        new BsonElement('timestamp', new BsonTimestamp(0x12345678, 5)),
                        new BsonElement('undefined', new BsonUndefined()),
                        new BsonElement('binary', new BsonBinary((byte) 80, [5, 4, 3, 2, 1] as byte[])),
                        new BsonElement('array', new BsonArray([new BsonInt32(1), new BsonInt64(2L), new BsonBoolean(true),
                                                                new BsonArray([new BsonInt32(1), new BsonInt32(2), new BsonInt32(3)]),
                                                                new BsonDocument('a', new BsonInt64(2L))])),
                        new BsonElement('document', new BsonDocument('a', new BsonInt32(1)))
                ])

        doc.with {
//        put('dbPointer', new DBPointer('foo.bar', new ObjectId()))
//            put('codeWithScope', new CodeWithScope('int x = y', new Document('y', 1)))
        }
        when:
        BsonBinaryWriter writer = new BsonBinaryWriter(new BasicOutputBuffer())
        new BsonDocumentCodec().encode(writer, doc, EncoderContext.builder().build())
        BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(
                new ByteBufNIO(ByteBuffer.wrap(writer.bsonOutput.toByteArray()))))
        def decodedDoc = new BsonDocumentCodec().decode(reader, DecoderContext.builder().build())

        then:
        decodedDoc.get('null') == doc.get('null')
        decodedDoc.get('int32') == doc.get('int32')
        decodedDoc.get('int64') == doc.get('int64')
        decodedDoc.get('decimal128') == doc.get('decimal128')
        decodedDoc.get('boolean') == doc.get('boolean')
        decodedDoc.get('date') == doc.get('date')
//        decodedDoc.get('dbPointer') == doc.get('dbPointer')
        decodedDoc.get('double') == doc.get('double')
        decodedDoc.get('minKey') == doc.get('minKey')
        decodedDoc.get('maxKey') == doc.get('maxKey')
        decodedDoc.get('javaScript') == doc.get('javaScript')
        decodedDoc.get('codeWithScope') == doc.get('codeWithScope')
        decodedDoc.get('objectId') == doc.get('objectId')
        decodedDoc.get('regex') == doc.get('regex')
        decodedDoc.get('string') == doc.get('string')
        decodedDoc.get('symbol') == doc.get('symbol')
        decodedDoc.get('timestamp') == doc.get('timestamp')
        decodedDoc.get('undefined') == doc.get('undefined')
        decodedDoc.get('binary') == doc.get('binary')
        decodedDoc.get('array') == doc.get('array')
        decodedDoc.get('document') == doc.get('document')
    }

    def 'should respect encodeIdFirst property in encoder context'() {
        given:
        def doc = new BsonDocument(
                [
                        new BsonElement('x', new BsonInt32(2)),
                        new BsonElement('_id', new BsonInt32(2)),
                        new BsonElement('nested', new BsonDocument(
                                [
                                        new BsonElement('x', new BsonInt32(2)),
                                        new BsonElement('_id', new BsonInt32(2))
                                ])),
                        new BsonElement('array', new BsonArray(asList(new BsonDocument(
                                [
                                        new BsonElement('x', new BsonInt32(2)),
                                        new BsonElement('_id', new BsonInt32(2))
                                ]
                        ))))
                ])

        when:
        def encodedDocument = new BsonDocument()
        new BsonDocumentCodec().encode(new BsonDocumentWriter(encodedDocument), doc,
                                       EncoderContext.builder().isEncodingCollectibleDocument(true).build())

        then:
        encodedDocument.keySet() as List == ['_id', 'x', 'nested', 'array']
        encodedDocument.getDocument('nested').keySet() as List == ['x', '_id']
        encodedDocument.getArray('array').get(0).asDocument().keySet() as List == ['x', '_id']

        when:
        encodedDocument.clear()
        new BsonDocumentCodec().encode(new BsonDocumentWriter(encodedDocument), doc,
                                       EncoderContext.builder().isEncodingCollectibleDocument(false).build())

        then:
        encodedDocument.keySet() as List == ['x', '_id', 'nested', 'array']
        encodedDocument.getDocument('nested').keySet() as List == ['x', '_id']
        encodedDocument.getArray('array').get(0).asDocument().keySet() as List == ['x', '_id']
    }

    def 'should encode nested raw documents'() {
        given:
        def doc = new BsonDocument('a', BsonBoolean.TRUE)
        def rawDoc = new RawBsonDocument(doc, new BsonDocumentCodec());
        def docWithNestedRawDoc = new BsonDocument('a', rawDoc).append('b', new BsonArray(asList(rawDoc)))

        when:
        def encodedDocument = new BsonDocument()
        new BsonDocumentCodec().encode(new BsonDocumentWriter(encodedDocument), docWithNestedRawDoc,
                                       EncoderContext.builder().isEncodingCollectibleDocument(true).build())

        then:
        encodedDocument == docWithNestedRawDoc
    }

    def 'should determine if document has an id'() {
        expect:
        !new BsonDocumentCodec().documentHasId(new BsonDocument());
        new BsonDocumentCodec().documentHasId(new BsonDocument('_id', new BsonInt32(1)));
    }

    def 'should get document id'() {
        expect:
        !new BsonDocumentCodec().getDocumentId(new BsonDocument());
        new BsonDocumentCodec().getDocumentId(new BsonDocument('_id', new BsonInt32(1))) == new BsonInt32(1)
    }

    def 'should generate document id if absent'() {
        given:
        def document = new BsonDocument()

        when:
        document = new BsonDocumentCodec().generateIdIfAbsentFromDocument(document);

        then:
        document.get('_id') instanceof BsonObjectId
    }

    def 'should not generate document id if present'() {
        given:
        def document = new BsonDocument('_id', new BsonInt32(1))

        when:
        document = new BsonDocumentCodec().generateIdIfAbsentFromDocument(document);

        then:
        document.get('_id') == new BsonInt32(1)
    }

}
