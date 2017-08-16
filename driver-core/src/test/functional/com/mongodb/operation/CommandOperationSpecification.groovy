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

package com.mongodb.operation

import category.Async
import category.Slow
import com.mongodb.MongoExecutionTimeoutException
import com.mongodb.OperationFunctionalSpecification
import org.bson.BsonBinary
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.codecs.BsonDocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isSharded

class CommandOperationSpecification extends OperationFunctionalSpecification {

    def 'should execute read command'() {
        given:
        def commandOperation = new CommandReadOperation<BsonDocument>(getNamespace().databaseName,
                                                                      new BsonDocument('count', new BsonString(getCollectionName())),
                                                                      new BsonDocumentCodec())
        when:
        def result = commandOperation.execute(getBinding())

        then:
        result.getNumber('n').intValue() == 0
    }

    @Category(Async)
    def 'should execute read command asynchronously'() {
        given:
        def commandOperation = new CommandReadOperation<BsonDocument>(getNamespace().databaseName,
                                                                      new BsonDocument('count', new BsonString(getCollectionName())),
                                                                      new BsonDocumentCodec())
        when:
        def result = executeAsync(commandOperation)

        then:
        result.getNumber('n').intValue() == 0

    }

    def 'should execute write command'() {
        when:
        def result = new CommandWriteOperation<BsonDocument>(getNamespace().databaseName,
                                                             new BsonDocument('findAndModify', new BsonString(getNamespace().fullName))
                                                                     .append('query', new BsonDocument('_id', new BsonInt32(42)))
                                                                     .append('update',
                                                                             new BsonDocument('_id', new BsonInt32(42))
                                                                                     .append('b', new BsonInt32(42))),
                                                             new BsonDocumentCodec())
                .execute(getBinding())

        then:
        result.containsKey('value')
    }

    @Category(Async)
    def 'should execute write command asynchronously'() {
        when:
        def result = executeAsync(new CommandWriteOperation<BsonDocument>(getNamespace().databaseName,
                                                             new BsonDocument('findAndModify', new BsonString(getNamespace().fullName))
                                                                     .append('query', new BsonDocument('_id', new BsonInt32(42)))
                                                                     .append('update',
                                                                             new BsonDocument('_id', new BsonInt32(42))
                                                                                     .append('b', new BsonInt32(42))),
                                                             new BsonDocumentCodec()))

        then:
        result.containsKey('value')
    }

    @Category(Slow)
    def 'should execute command larger than 16MB'() {
        when:
        def result = new CommandWriteOperation<BsonDocument>(getNamespace().databaseName,
                                                             new BsonDocument('findAndModify', new BsonString(getNamespace().fullName))
                                                                     .append('query', new BsonDocument('_id', new BsonInt32(42)))
                                                                     .append('update',
                                                                             new BsonDocument('_id', new BsonInt32(42))
                                                                                     .append('b', new BsonBinary(
                                                                                     new byte[16 * 1024 * 1024 - 30]))),
                                                             new BsonDocumentCodec())
                .execute(getBinding())

        then:
        result.containsKey('value')
    }

    @IgnoreIf({ isSharded() })
    def 'should throw execution timeout exception from execute'() {
        given:
        def commandOperation = new CommandReadOperation<BsonDocument>(getNamespace().databaseName,
                                                                      new BsonDocument('count', new BsonString(getCollectionName()))
                                                                              .append('maxTimeMS', new BsonInt32(1)),
                                                                      new BsonDocumentCodec())
        enableMaxTimeFailPoint()

        when:
        commandOperation.execute(getBinding())

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    @Category(Async)
    @IgnoreIf({ isSharded() })
    def 'should throw execution timeout exception from executeAsync'() {
        given:
        def commandOperation = new CommandReadOperation<BsonDocument>(getNamespace().databaseName,
                                                                      new BsonDocument('count', new BsonString(getCollectionName()))
                                                                              .append('maxTimeMS', new BsonInt32(1)),
                                                                      new BsonDocumentCodec())
        enableMaxTimeFailPoint()

        when:
        executeAsync(commandOperation)

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }
}
