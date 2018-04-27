/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal.operation

import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import spock.lang.Specification

import static com.mongodb.internal.operation.WriteConcernHelper.createWriteConcernError

class WriteConcernHelperSpecification extends Specification {

    def 'should create write concern error'() {
        when:
        def writeConcernError = createWriteConcernError(new BsonDocument('code', new BsonInt32(42))
                .append('errmsg', new BsonString('a timeout'))
                .append('errInfo', new BsonDocument('wtimeout', new BsonInt32(1))))

        then:
        writeConcernError.getCode() == 42
        writeConcernError.getCodeName() == ''
        writeConcernError.getMessage() == 'a timeout'
        writeConcernError.getDetails() == new BsonDocument('wtimeout', new BsonInt32(1))

        when:
        writeConcernError = createWriteConcernError(new BsonDocument('code', new BsonInt32(42))
                .append('codeName', new BsonString('TimeoutError'))
                .append('errmsg', new BsonString('a timeout'))
                .append('errInfo', new BsonDocument('wtimeout', new BsonInt32(1))))

        then:
        writeConcernError.getCode() == 42
        writeConcernError.getCodeName() == 'TimeoutError'
        writeConcernError.getMessage() == 'a timeout'
        writeConcernError.getDetails() == new BsonDocument('wtimeout', new BsonInt32(1))
    }
}
