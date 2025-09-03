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

package com.mongodb.internal.operation

import com.mongodb.MongoCommandException
import com.mongodb.ServerAddress
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import spock.lang.Specification

import static com.mongodb.internal.operation.CommandOperationHelper.isNamespaceError
import static com.mongodb.internal.operation.CommandOperationHelper.rethrowIfNotNamespaceError

class CommandOperationHelperSpecification extends Specification {

    def 'should be a namespace error if Throwable is a MongoCommandException and error code is 26'() {
        expect:
        isNamespaceError(new MongoCommandException(new BsonDocument('ok', BsonBoolean.FALSE)
                                                           .append('code', new BsonInt32(26)),
                                                   new ServerAddress()))
    }

    def 'should be a namespace error if Throwable is a MongoCommandException and error message contains "ns not found"'() {
        expect:
        isNamespaceError(new MongoCommandException(new BsonDocument('ok', BsonBoolean.FALSE)
                                                           .append('errmsg', new BsonString('the ns not found here')),
                                                   new ServerAddress()))
    }

    def 'should not be a namespace error if Throwable is a MongoCommandException and error message does not contain "ns not found"'() {
        expect:
        !isNamespaceError(new MongoCommandException(new BsonDocument('ok', BsonBoolean.FALSE)
                                                           .append('errmsg', new BsonString('some other error')),
                                                   new ServerAddress()))
    }

    def 'should not be a namespace error should return false if Throwable is not a MongoCommandException'() {
        expect:
        !isNamespaceError(new NullPointerException())
    }

    def 'should rethrow if not namespace error'() {
        when:
        rethrowIfNotNamespaceError(new MongoCommandException(new BsonDocument('ok', BsonBoolean.FALSE)
                                                                     .append('errmsg', new BsonString('some other error')),
                                                             new ServerAddress()))

        then:
        thrown(MongoCommandException)

        when:
        rethrowIfNotNamespaceError(new MongoCommandException(new BsonDocument('ok', BsonBoolean.FALSE)
                                                                     .append('errmsg', new BsonString('some other error')),
                                                             new ServerAddress()), 'some value')

        then:
        thrown(MongoCommandException)
    }

    def 'should not rethrow if namespace error'() {
        when:
        rethrowIfNotNamespaceError(new MongoCommandException(new BsonDocument('ok', BsonBoolean.FALSE)
                                                                     .append('code', new BsonInt32(26)),
                                                             new ServerAddress()))

        then:
        true
    }

    def 'should return default value if not namespace error'() {
        expect:
        rethrowIfNotNamespaceError(new MongoCommandException(new BsonDocument('ok', BsonBoolean.FALSE)
                                                                     .append('code', new BsonInt32(26)),
                                                             new ServerAddress()), 'some value') == 'some value'
    }

}
