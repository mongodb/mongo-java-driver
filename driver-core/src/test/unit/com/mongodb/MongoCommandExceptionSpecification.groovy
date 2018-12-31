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

package com.mongodb

import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import spock.lang.Specification

class MongoCommandExceptionSpecification extends Specification {

    def 'should extract error message'() {
        expect:
        new MongoCommandException(new BsonDocument('ok', BsonBoolean.FALSE).append('errmsg', new BsonString('the error message')),
                                  new ServerAddress())
                .getErrorMessage() == 'the error message'
        new MongoCommandException(new BsonDocument('ok', BsonBoolean.FALSE),
                                  new ServerAddress())
                .getErrorMessage() == ''
    }

    def 'should extract error code'() {
        expect:
        new MongoCommandException(new BsonDocument('ok', BsonBoolean.FALSE).append('code', new BsonInt32(26)),
                                  new ServerAddress())
                .getErrorCode() == 26
        new MongoCommandException(new BsonDocument('ok', BsonBoolean.FALSE),
                                  new ServerAddress())
                .getErrorCode() == -1
    }

    def 'should extract error code name'() {
        expect:
        new MongoCommandException(new BsonDocument('ok', BsonBoolean.FALSE).append('code', new BsonInt32(26))
                .append('codeName', new BsonString('TimeoutError')), new ServerAddress()).getErrorCodeName() == 'TimeoutError'
        new MongoCommandException(new BsonDocument('ok', BsonBoolean.FALSE), new ServerAddress()).getErrorCodeName() == ''
    }

    def 'should create message'() {
        expect:
        new MongoCommandException(new BsonDocument('ok', BsonBoolean.FALSE).append('code', new BsonInt32(26))
                .append('codeName', new BsonString('TimeoutError')).append('errmsg', new BsonString('the error message')),
                new ServerAddress())
                .getMessage() == 'Command failed with error 26 (TimeoutError): \'the error message\' on server 127.0.0.1:27017. ' +
                'The full response is {"ok": false, "code": 26, "codeName": "TimeoutError", "errmsg": "the error message"}'
        new MongoCommandException(new BsonDocument('ok', BsonBoolean.FALSE).append('code', new BsonInt32(26))
                .append('errmsg', new BsonString('the error message')), new ServerAddress())
                .getMessage() == 'Command failed with error 26: \'the error message\' on server 127.0.0.1:27017. ' +
                'The full response is {"ok": false, "code": 26, "errmsg": "the error message"}'
    }
}
