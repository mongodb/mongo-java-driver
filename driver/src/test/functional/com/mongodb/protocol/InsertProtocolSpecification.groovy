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

package com.mongodb.protocol

import com.mongodb.WriteConcern
import org.bson.BsonArray
import org.bson.BsonBinary
import org.bson.BsonBoolean
import org.bson.BsonDateTime
import org.bson.BsonDbPointer
import org.bson.BsonDocument
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
import org.bson.codecs.BsonDocumentCodec
import org.bson.types.ObjectId
import org.mongodb.FunctionalSpecification
import org.mongodb.operation.InsertRequest
import org.mongodb.operation.QueryFlag

import static org.mongodb.Fixture.getBinding

class InsertProtocolSpecification extends FunctionalSpecification {
    def 'should insert all types'() {
        given:
        def doc = new BsonDocument(
                [
                        new BsonElement('_id', new BsonObjectId(new ObjectId())),
                        new BsonElement('null', new BsonNull()),
                        new BsonElement('int32', new BsonInt32(42)),
                        new BsonElement('int64', new BsonInt64(52L)),
                        new BsonElement('boolean', new BsonBoolean(true)),
                        new BsonElement('date', new BsonDateTime(new Date().getTime())),
                        new BsonElement('double', new BsonDouble(62.0)),
                        new BsonElement('string', new BsonString('the fox ...')),
                        new BsonElement('minKey', new BsonMinKey()),
                        new BsonElement('maxKey', new BsonMaxKey()),
                        new BsonElement('dbPointer', new BsonDbPointer('test.test', new ObjectId())),
                        new BsonElement('code', new BsonJavaScript('int i = 0;')),
                        new BsonElement('codeWithScope', new BsonJavaScriptWithScope('x', new BsonDocument('x', new BsonInt32(1)))),
                        new BsonElement('objectId', new BsonObjectId(new ObjectId())),
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



        when:
        def connection = getBinding().getWriteConnectionSource().getConnection()
        new InsertProtocol<BsonDocument>(getNamespace(), true, WriteConcern.ACKNOWLEDGED, [new InsertRequest<BsonDocument>(doc)],
                new BsonDocumentCodec()).execute(connection);


        then:
        new QueryProtocol<BsonDocument>(getNamespace(), EnumSet.noneOf(QueryFlag), 0, 1, new BsonDocument(), new BsonDocument(),
                                        new BsonDocumentCodec()).execute(connection).getResults()[0]

        cleanup:
        connection.release();
    }
}