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
import com.mongodb.MongoExecutionTimeoutException
import com.mongodb.OperationFunctionalSpecification
import org.bson.BsonBinary
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.junit.experimental.categories.Category

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.Arrays.asList
import static org.junit.Assume.assumeFalse
import static org.junit.Assume.assumeTrue

class CommandOperationSpecification extends OperationFunctionalSpecification {

    def 'should execute command'() {
        given:
        def commandOperation = new CommandReadOperation(getNamespace().databaseName,
                                                        new BsonDocument('count', new BsonString(getCollectionName()))
        )
        when:
        def result = commandOperation.execute(getBinding())

        then:
        result.response.getNumber('n').intValue() == 0
    }

    def 'should execute command larger than 16MB'() {
        when:
        def result = new CommandReadOperation(getNamespace().databaseName,
                                              new BsonDocument('findAndModify', new BsonString(getNamespace().fullName))
                                                      .append('query', new BsonDocument('_id', new BsonInt32(42)))
                                                      .append('update',
                                                              new BsonDocument('_id', new BsonInt32(42))
                                                                      .append('b', new BsonBinary(new byte[16 * 1024 * 1024 - 30])))
        )
                .execute(getBinding())

        then:
        result.response.containsKey('value')
    }

    def 'should execute command asynchronously'() {
        given:
        def commandOperation = new CommandReadOperation(getNamespace().databaseName,
                                                        new BsonDocument('count', new BsonString(getCollectionName()))
        )
        when:
        def result = commandOperation.executeAsync(getAsyncBinding()).get()

        then:
        result.response.getNumber('n').intValue() == 0

    }

    def 'should throw execution timeout exception from execute'() {
        assumeFalse(isSharded())
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)))

        given:
        def commandOperation = new CommandReadOperation(getNamespace().databaseName,
                                                        new BsonDocument('count', new BsonString(getCollectionName()))
                                                                .append('maxTimeMS', new BsonInt32(1))
        )
        enableMaxTimeFailPoint()

        when:
        commandOperation.execute(getBinding())

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    @Category(Async)
    def 'should throw execution timeout exception from executeAsync'() {
        assumeFalse(isSharded())
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)))

        given:
        def commandOperation = new CommandReadOperation(getNamespace().databaseName,
                                                        new BsonDocument('count', new BsonString(getCollectionName()))
                                                                .append('maxTimeMS', new BsonInt32(1))
        )
        enableMaxTimeFailPoint()

        when:
        commandOperation.executeAsync(getAsyncBinding()).get()

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }
}
