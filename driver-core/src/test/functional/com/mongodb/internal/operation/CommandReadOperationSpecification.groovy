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

import com.mongodb.MongoExecutionTimeoutException
import com.mongodb.OperationFunctionalSpecification
import org.bson.BsonBinary
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.codecs.BsonDocumentCodec
import spock.lang.IgnoreIf
import util.spock.annotations.Slow

import static com.mongodb.ClusterFixture.DEFAULT_CSOT_FACTORY
import static com.mongodb.ClusterFixture.MAX_TIME_MS_CSOT_FACTORY
import static com.mongodb.ClusterFixture.NO_CSOT_FACTORY
import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isSharded

class CommandReadOperationSpecification extends OperationFunctionalSpecification {

    def 'should execute read command'() {
        given:
        def commandOperation = new CommandReadOperation<BsonDocument>(DEFAULT_CSOT_FACTORY, getNamespace().databaseName,
                                                                      new BsonDocument('count', new BsonString(getCollectionName())),
                                                                      new BsonDocumentCodec())
        when:
        def result = commandOperation.execute(getBinding())

        then:
        result.getNumber('n').intValue() == 0
    }


    def 'should execute read command asynchronously'() {
        given:
        def commandOperation = new CommandReadOperation<BsonDocument>(DEFAULT_CSOT_FACTORY, getNamespace().databaseName,
                                                                      new BsonDocument('count', new BsonString(getCollectionName())),
                                                                      new BsonDocumentCodec())
        when:
        def result = executeAsync(commandOperation)

        then:
        result.getNumber('n').intValue() == 0
    }

    @Slow
    def 'should execute command larger than 16MB'() {
        when:
        def result = new CommandReadOperation<>(DEFAULT_CSOT_FACTORY, getNamespace().databaseName,
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
    def 'will not throw execution timeout exception if no timeoutMS is set'() {
        given:
        def operation = new CommandReadOperation<BsonDocument>(csotFactory, getNamespace().databaseName,
                new BsonDocument('count', new BsonString(getCollectionName())),
                new BsonDocumentCodec())
        enableMaxTimeFailPoint()

        when:
        execute(operation, async)

        then:
        notThrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()

        where:
        [async, csotFactory] << [[true, false], [MAX_TIME_MS_CSOT_FACTORY, NO_CSOT_FACTORY]].combinations()
    }

    @IgnoreIf({ isSharded() })
    def 'should throw execution timeout exception from execute if timeoutMS is set'() {
        given:
        def operation = new CommandReadOperation<BsonDocument>(DEFAULT_CSOT_FACTORY, getNamespace().databaseName,
                new BsonDocument('count', new BsonString(getCollectionName())),
                new BsonDocumentCodec())
        enableMaxTimeFailPoint()

        when:
        execute(operation, async)

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()

        where:
        async << [true, false]
    }

    @IgnoreIf({ isSharded() })
    def 'should throw execution timeout exception from execute if maxTimeMS is explicitly set'() {
        given:
        def operation = new CommandReadOperation<BsonDocument>(NO_CSOT_FACTORY, getNamespace().databaseName,
                new BsonDocument('count', new BsonString(getCollectionName()))
                .append('maxTimeMS', new BsonInt64(100)),
                new BsonDocumentCodec())
        enableMaxTimeFailPoint()

        when:
        execute(operation, async)

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()

        where:
        async << [true, false]
    }
}
