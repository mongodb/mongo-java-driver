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

package org.bson
import org.bson.codecs.BsonDocumentCodec
import org.bson.types.Binary
import org.bson.types.BsonArray
import org.bson.types.BsonBoolean
import org.bson.types.BsonDateTime
import org.bson.types.BsonDocument
import org.bson.types.BsonDouble
import org.bson.types.BsonElement
import org.bson.types.BsonInt32
import org.bson.types.BsonInt64
import org.bson.types.BsonNull
import org.bson.types.BsonString
import org.bson.types.Code
import org.bson.types.CodeWithScope
import org.bson.types.DBPointer
import org.bson.types.MaxKey
import org.bson.types.MinKey
import org.bson.types.ObjectId
import org.bson.types.RegularExpression
import org.bson.types.Symbol
import org.bson.types.Timestamp
import org.bson.types.Undefined
import spock.lang.Specification

class BsonDocumentWriterSpecification extends Specification {
    def 'should write all types'() {
        given:
        def doc = new BsonDocument(
                [
                        new BsonElement('null', new BsonNull()),
                        new BsonElement('int32', new BsonInt32(42)),
                        new BsonElement('int64', new BsonInt64(52L)),
                        new BsonElement('boolean', new BsonBoolean(true)),
                        new BsonElement('date', new BsonDateTime(new Date().getTime())),
                        new BsonElement('double', new BsonDouble(62.0)),
                        new BsonElement('string', new BsonString('the fox ...')),
                        new BsonElement('minKey', new MinKey()),
                        new BsonElement('maxKey', new MaxKey()),
                        new BsonElement('dbPointer', new DBPointer("test.test", new ObjectId())),
                        new BsonElement('code', new Code('int i = 0;')),
                        new BsonElement('codeWithScope', new CodeWithScope('x', new BsonDocument('x', new BsonInt32(1)))),
                        new BsonElement('objectId', new ObjectId()),
                        new BsonElement('regex', new RegularExpression('^test.*regex.*xyz$', 'i')),
                        new BsonElement('symbol', new Symbol('ruby stuff')),
                        new BsonElement('timestamp', new Timestamp(0x12345678, 5)),
                        new BsonElement('undefined', new Undefined()),
                        new BsonElement('binary', new Binary((byte) 80, [5, 4, 3, 2, 1] as byte[])),
                        new BsonElement('array', new BsonArray([new BsonInt32(1), new BsonInt64(2L), new BsonBoolean(true),
                                                                new BsonArray([new BsonInt32(1), new BsonInt32(2), new BsonInt32(3)]),
                                                                new BsonDocument('a', new BsonInt64(2L))])),
                        new BsonElement('document', new BsonDocument('a', new BsonInt32(1)))
                ])


        when:
        def encodedDoc = new BsonDocument();
        new BsonDocumentCodec().encode(new BsonDocumentWriter(encodedDoc), doc)

        then:
        encodedDoc == doc
    }

}