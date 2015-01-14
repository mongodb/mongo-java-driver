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
import com.mongodb.async.FutureResultCallback
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.concurrent.TimeUnit.MILLISECONDS

class ListDatabasesOperationSpecification extends OperationFunctionalSpecification {
    def codec = new DocumentCodec()

    def 'should return a list of database names'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('_id', 1))
        def operation = new ListDatabasesOperation(codec)

        when:
        def names = operation.execute(getBinding()).next()*.get('name')


        then:
        names.contains(getDatabaseName())
    }

    @Category(Async)
    def 'should return a list of database names asynchronously'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('_id', 1))
        def operation = new ListDatabasesOperation(codec)

        when:
        def callback = new FutureResultCallback()
        def cursor = executeAsync(operation)
        cursor.next(callback)
        def names = callback.get()*.get('name')

        then:
        names.contains(getDatabaseName())
    }

    @IgnoreIf({ isSharded() || !serverVersionAtLeast([2, 6, 0]) })
    def 'should throw execution timeout exception from execute'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document())
        def operation = new ListDatabasesOperation(codec).maxTime(1000, MILLISECONDS)

        enableMaxTimeFailPoint()

        when:
        operation.execute(getBinding())

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    @Category(Async)
    @IgnoreIf({ isSharded() || !serverVersionAtLeast([2, 6, 0]) })
    def 'should throw execution timeout exception from executeAsync'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document())
        def operation = new ListDatabasesOperation(codec).maxTime(1000, MILLISECONDS)

        enableMaxTimeFailPoint()

        when:
        executeAsync(operation);

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }
}
