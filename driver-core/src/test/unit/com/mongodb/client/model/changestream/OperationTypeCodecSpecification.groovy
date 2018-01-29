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

package com.mongodb.client.model.changestream

import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.BsonReader
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import spock.lang.Specification

class OperationTypeCodecSpecification extends Specification {

    def 'should round trip OperationType successfully'() {
        when:
        def codec = new OperationTypeCodec()

        then:
        codec.getEncoderClass() == OperationType

        when:
        def writer = new BsonDocumentWriter(new BsonDocument())
        writer.writeStartDocument()
        writer.writeName('operationType')
        codec.encode(writer, operationType, EncoderContext.builder().build())
        writer.writeEndDocument()

        then:
        operationType.getValue() == writer.getDocument().getString('operationType').getValue()

        when:
        BsonReader bsonReader = new BsonDocumentReader(writer.getDocument())
        bsonReader.readStartDocument()
        bsonReader.readName()
        OperationType actual = codec.decode(bsonReader, DecoderContext.builder().build())

        then:
        operationType == actual

        where:
        operationType << [
                OperationType.DELETE,
                OperationType.INSERT,
                OperationType.INVALIDATE,
                OperationType.REPLACE,
                OperationType.UPDATE
        ]
    }
}
