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

package com.mongodb.internal.connection

import org.bson.BsonArray
import org.bson.BsonBinaryReader
import org.bson.BsonBinaryWriter
import org.bson.BsonDocument
import org.bson.BsonObjectId
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import org.bson.io.BasicOutputBuffer
import org.bson.io.BsonOutput
import spock.lang.Specification

import static org.bson.BsonHelper.documentWithValuesOfEveryType
import static org.bson.BsonHelper.getBsonValues

class IdHoldingBsonWriterSpecification extends Specification {

    def 'should write all types'() {
        given:
        def bsonBinaryWriter = new BsonBinaryWriter(new BasicOutputBuffer())
        def idTrackingBsonWriter = new IdHoldingBsonWriter(bsonBinaryWriter)
        def document = documentWithValuesOfEveryType()

        when:
        new BsonDocumentCodec().encode(idTrackingBsonWriter, document, EncoderContext.builder().build())
        def encodedDocument = getEncodedDocument(bsonBinaryWriter.getBsonOutput())

        then:
        !document.containsKey('_id')
        encodedDocument.containsKey('_id')
        idTrackingBsonWriter.getId() == encodedDocument.get('_id')

        when:
        encodedDocument.remove('_id')

        then:
        encodedDocument == document
    }

    def 'should support all types for _id value'() {
        given:
        def bsonBinaryWriter = new BsonBinaryWriter(new BasicOutputBuffer())
        def idTrackingBsonWriter = new IdHoldingBsonWriter(bsonBinaryWriter)
        def document = new BsonDocument()
        document.put('_id', id)

        when:
        new BsonDocumentCodec().encode(idTrackingBsonWriter, document, EncoderContext.builder().build())
        def encodedDocument = getEncodedDocument(bsonBinaryWriter.getBsonOutput())

        then:
        encodedDocument == document
        idTrackingBsonWriter.getId() == id

        where:
        id << getBsonValues()
    }

    def 'serialize document with list of documents that contain an _id field'() {
        def bsonBinaryWriter = new BsonBinaryWriter(new BasicOutputBuffer())
        def idTrackingBsonWriter = new IdHoldingBsonWriter(bsonBinaryWriter)
        def document = new BsonDocument('_id', new BsonObjectId())
                .append('items', new BsonArray(Collections.singletonList(new BsonDocument('_id', new BsonObjectId()))))

        when:
        new BsonDocumentCodec().encode(idTrackingBsonWriter, document, EncoderContext.builder().build())
        def encodedDocument = getEncodedDocument(bsonBinaryWriter.getBsonOutput())

        then:
        encodedDocument == document
    }

    def 'serialize _id documents containing arrays'() {
        def bsonBinaryWriter = new BsonBinaryWriter(new BasicOutputBuffer())
        def idTrackingBsonWriter = new IdHoldingBsonWriter(bsonBinaryWriter)
        BsonDocument document = BsonDocument.parse(json)

        when:
        new BsonDocumentCodec().encode(idTrackingBsonWriter, document, EncoderContext.builder()
                .isEncodingCollectibleDocument(true).build())
        def encodedDocument = getEncodedDocument(bsonBinaryWriter.getBsonOutput())

        then:
        encodedDocument == document

        where:
        json << ['{"_id": {"a": []}, "b": 123}',
                 '{"_id": {"a": [1, 2]}, "b": 123}',
                 '{"_id": {"a": [[[[1]]]]}, "b": 123}',
                 '{"_id": {"a": [{"a": [1, 2]}]}, "b": 123}',
                 '{"_id": {"a": {"a": [1, 2]}}, "b": 123}',
                 '{"_id": {"a": [1, 2], "b": [123]}}',
                 '{"_id": [], "b": 123}',
                 '{"_id": [1, 2], "b": 123}',
                 '{"_id": [[1], [[2]]], "b": 123}',
                 '{"_id": [{"a": 1}], "b": 123}',
                 '{"_id": [{"a": [{"b": 123}]}]}']
    }

    private static BsonDocument getEncodedDocument(BsonOutput buffer) {
        new BsonDocumentCodec().decode(new BsonBinaryReader(buffer.getByteBuffers().get(0).asNIO()),
                DecoderContext.builder().build())
    }
}
