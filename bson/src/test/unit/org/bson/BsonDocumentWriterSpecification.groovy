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

package org.bson

import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.EncoderContext
import spock.lang.Specification

import static org.bson.BsonHelper.documentWithValuesOfEveryType

class BsonDocumentWriterSpecification extends Specification {

    def 'should write all types'() {
        when:
        def encodedDoc = new BsonDocument();
        new BsonDocumentCodec().encode(new BsonDocumentWriter(encodedDoc), documentWithValuesOfEveryType(),
                EncoderContext.builder().build())

        then:
        encodedDoc == documentWithValuesOfEveryType()
    }

    def 'should pipe all types'() {
        given:
        def document = new BsonDocument()
        def reader = new BsonDocumentReader(documentWithValuesOfEveryType())
        def writer = new BsonDocumentWriter(document)

        when:
        writer.pipe(reader)

        then:
        document == documentWithValuesOfEveryType()
    }

    def 'should pipe all types with extra elements'() {
        given:
        def document = new BsonDocument()
        def reader = new BsonDocumentReader(new BsonDocument())
        def writer = new BsonDocumentWriter(document)

        def extraElements = []
        for (def entry : documentWithValuesOfEveryType()) {
            extraElements.add(new BsonElement(entry.getKey(), entry.getValue()))
        }

        when:
        writer.pipe(reader, extraElements)

        then:
        document == documentWithValuesOfEveryType()
    }
}