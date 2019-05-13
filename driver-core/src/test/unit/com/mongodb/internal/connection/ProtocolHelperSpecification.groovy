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

import com.mongodb.DuplicateKeyException
import com.mongodb.MongoCommandException
import com.mongodb.MongoExecutionTimeoutException
import com.mongodb.MongoNodeIsRecoveringException
import com.mongodb.MongoNotPrimaryException
import com.mongodb.MongoQueryException
import com.mongodb.ServerAddress
import com.mongodb.WriteConcernException
import com.mongodb.WriteConcernResult
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonNull
import org.bson.BsonString
import spock.lang.Specification

import static com.mongodb.internal.connection.ProtocolHelper.getCommandFailureException
import static com.mongodb.internal.connection.ProtocolHelper.getQueryFailureException
import static com.mongodb.internal.connection.ProtocolHelper.isCommandOk

class ProtocolHelperSpecification extends Specification {

    def 'isCommandOk should be false if ok field is missing'() {
        expect:
        !isCommandOk(new BsonDocument())
    }

    def 'isCommandOk should be false for numbers that are 0'() {
        expect:
        !isCommandOk(new BsonDocument('ok', new BsonInt32(0)))
        !isCommandOk(new BsonDocument('ok', new BsonInt64(0)))
        !isCommandOk(new BsonDocument('ok', new BsonDouble(0.0)))
    }

    def 'isCommandOk should be true for numbers that are not 0'() {
        expect:
        !isCommandOk(new BsonDocument('ok', new BsonInt32(10)))
        !isCommandOk(new BsonDocument('ok', new BsonInt64(10)))
        !isCommandOk(new BsonDocument('ok', new BsonDouble(10.0)))
    }

    def 'isCommandOk should equal the boolean value'() {
        expect:
        isCommandOk(new BsonDocument('ok', BsonBoolean.TRUE))
        !isCommandOk(new BsonDocument('ok', BsonBoolean.FALSE))
    }

    def 'isCommandOk should be false of ok is not a number or a boolean'() {
        expect:
        !isCommandOk(new BsonDocument('ok', new BsonNull()))
    }

    def 'command failure exception should be MongoExecutionTimeoutException if error code is 50'() {
        expect:
        getCommandFailureException(new BsonDocument('ok', new BsonInt32(0)).append('code', new BsonInt32(50)),
                                   new ServerAddress()) instanceof MongoExecutionTimeoutException
    }

    def 'query failure exception should be MongoExecutionTimeoutException if error code is 50'() {
        expect:
        getQueryFailureException(new BsonDocument('code', new BsonInt32(50)),
                                 new ServerAddress()) instanceof MongoExecutionTimeoutException
    }

    def 'command failure exceptions should handle MongoNotPrimaryException scenarios'() {
        expect:
        getCommandFailureException(exception, new ServerAddress()) instanceof MongoNotPrimaryException

        where:
        exception << [
                BsonDocument.parse('{ok: 0, errmsg: "not master server"}'),
                BsonDocument.parse('{ok: 0, code: 10107}'),
                BsonDocument.parse('{ok: 0, code: 13435}')
        ]
    }

    def 'query failure exceptions should handle MongoNotPrimaryException scenarios'() {
        expect:
        getQueryFailureException(exception, new ServerAddress()) instanceof MongoNotPrimaryException

        where:
        exception << [
                BsonDocument.parse('{$err: "not master server"}'),
                BsonDocument.parse('{code: 10107}'),
                BsonDocument.parse('{code: 13435}')
        ]
    }

    def 'command failure exceptions should handle MongoNodeIsRecoveringException scenarios'() {
        expect:
        getCommandFailureException(exception, new ServerAddress()) instanceof MongoNodeIsRecoveringException

        where:
        exception << [
                BsonDocument.parse('{ok: 0, errmsg: "node is recovering now"}'),
                BsonDocument.parse('{ok: 0, code: 11600}'),
                BsonDocument.parse('{ok: 0, code: 11602}'),
                BsonDocument.parse('{ok: 0, code: 13436}'),
                BsonDocument.parse('{ok: 0, code: 189}'),
                BsonDocument.parse('{ok: 0, code: 91}'),
        ]
    }

    def 'query failure exceptions should handle MongoNodeIsRecoveringException scenarios'() {
        expect:
        getQueryFailureException(exception, new ServerAddress()) instanceof MongoNodeIsRecoveringException

        where:
        exception << [
                BsonDocument.parse('{$err: "node is recovering now"}'),
                BsonDocument.parse('{code: 11600}'),
                BsonDocument.parse('{code: 11602}'),
                BsonDocument.parse('{code: 13436}'),
                BsonDocument.parse('{code: 189}'),
                BsonDocument.parse('{code: 91}'),
        ]
    }

    def 'command failure exception should be MongoCommandException'() {
        expect:
        getCommandFailureException(new BsonDocument('ok', new BsonInt32(0)).append('errmsg', new BsonString('some other problem')),
                                   new ServerAddress()) instanceof MongoCommandException
    }

    def 'query failure exception should be MongoQueryException'() {
        expect:
        getQueryFailureException(new BsonDocument('$err', new BsonString('some other problem')),
                                 new ServerAddress()) instanceof MongoQueryException
    }

}
