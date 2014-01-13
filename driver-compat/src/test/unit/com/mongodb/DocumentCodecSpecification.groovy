/*
 * Copyright (c) 2008 MongoDB, Inc.
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

package com.mongodb

import org.bson.BSONWriter
import org.bson.BasicBSONObject
import org.bson.types.CodeWScope
import org.bson.types.ObjectId
import org.mongodb.Document
import org.mongodb.codecs.PrimitiveCodecs
import spock.lang.Specification

class DocumentCodecSpecification extends Specification {

    def 'should encode driver-compat com.mongodb.DBRef type'() {
        given:
        ObjectId objectId = new ObjectId()
        Document document = new Document('a', new DBRef(null, 'foo', objectId))
        BSONWriter bsonWriter = Mock()

        when:
        new DocumentCodec(PrimitiveCodecs.createDefault()).encode(bsonWriter, document)

        then:
        1 * bsonWriter.writeStartDocument()
        1 * bsonWriter.writeName('a')
        1 * bsonWriter.writeStartDocument()
        1 * bsonWriter.writeString('$ref', 'foo')
        1 * bsonWriter.writeName('$id')
        1 * bsonWriter.writeObjectId(objectId)
        2 * bsonWriter.writeEndDocument()
    }

    def 'should encode driver-compat CodeWScope type'() {
        given:
        Document document = new Document('c', new CodeWScope('i++', new BasicBSONObject('i', 0)))
        BSONWriter bsonWriter = Mock()

        when:
        new DocumentCodec(PrimitiveCodecs.createDefault()).encode(bsonWriter, document)

        then:
        1 * bsonWriter.writeStartDocument()
        1 * bsonWriter.writeName('c')
        1 * bsonWriter.writeJavaScriptWithScope('i++')
        1 * bsonWriter.writeStartDocument()
        1 * bsonWriter.writeName('i')
        1 * bsonWriter.writeInt32(0)
        2 * bsonWriter.writeEndDocument()
    }
}
