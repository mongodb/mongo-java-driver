/*
 * Copyright (c) 2008 - 2014 MongoDB Inc. <http://mongodb.com>
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

package org.mongodb

import org.bson.types.BsonBoolean
import org.bson.types.BsonDocument
import org.bson.types.BsonDouble
import org.bson.types.BsonInt32
import org.bson.types.BsonString
import org.mongodb.connection.ServerAddress
import spock.lang.Specification

class CommandResultSpecification extends Specification {
    def 'when ok field is missing then ok property should be false'() {
        expect:
        !new CommandResult(new ServerAddress(), new BsonDocument(), 1).ok
    }

    def 'when ok field is false then ok property should be false'() {
        expect:
        !new CommandResult(new ServerAddress(), new BsonDocument('ok', BsonBoolean.FALSE), 1).ok
    }

    def 'when ok field is true then ok property should be true'() {
        expect:
        new CommandResult(new ServerAddress(), new BsonDocument('ok', BsonBoolean.TRUE), 1).ok
    }

    def 'when ok field is zero then ok property should be false'() {
        expect:
        !new CommandResult(new ServerAddress(), new BsonDocument('ok', new BsonDouble(0)), 1).ok
        !new CommandResult(new ServerAddress(), new BsonDocument('ok', new BsonInt32(0)), 1).ok
    }

    def 'when ok field is one then ok property should be true'() {
        expect:
        new CommandResult(new ServerAddress(), new BsonDocument('ok', new BsonDouble(1)), 1).ok
        new CommandResult(new ServerAddress(), new BsonDocument('ok', new BsonInt32(1)), 1).ok
    }

    def 'when code field is missing then code property should be -1'() {
        expect:
        new CommandResult(new ServerAddress(), new BsonDocument(), 1).errorCode == -1
    }

    def 'when code field is present then code property should be equal to it'() {
        expect:
        new CommandResult(new ServerAddress(), new BsonDocument('code', new BsonInt32(11000)), 1).errorCode == 11000
    }

    def 'when errmsg field is missing then errorMessage property should be null'() {
        expect:
        new CommandResult(new ServerAddress(), new BsonDocument(), 1).errorMessage == null
    }

    def 'when errmsg field is present then errorMessage property should equal to it'() {
        expect:
        new CommandResult(new ServerAddress(), new BsonDocument('errmsg', new BsonString('Hello')), 1).errorMessage == 'Hello'
    }
}