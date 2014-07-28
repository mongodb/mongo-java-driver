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
import com.mongodb.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import org.mongodb.Document

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.SECONDS
import static org.junit.Assume.assumeTrue

class CountOperationSpecification extends OperationFunctionalSpecification {

    private List<InsertRequest<Document>> insertDocumentList;

    def setup() {
        insertDocumentList = [
                new InsertRequest<Document>(new Document()),
                new InsertRequest<Document>(new Document()),
                new InsertRequest<Document>(new Document()),
                new InsertRequest<Document>(new Document()),
                new InsertRequest<Document>(new Document()),
                new InsertRequest<Document>(new Document()),
                new InsertRequest<Document>(new Document()),
                new InsertRequest<Document>(new Document())
        ]
        new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED, insertDocumentList, new DocumentCodec()).execute(getBinding())
    }

    def 'should get the count'() {
        expect:
        new CountOperation(getNamespace(), new Find()).execute(getBinding()) == insertDocumentList.size()
    }

    @Category(Async)
    def 'should get the count asynchronously'() {
        expect:
        new CountOperation(getNamespace(), new Find()).executeAsync(getAsyncBinding()).get() ==
        insertDocumentList.size()
    }


    def 'should throw execution timeout exception from execute'() {
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)))

        given:
        def find = new Find().maxTime(1, SECONDS)
        def countOperation = new CountOperation(getNamespace(), find)
        enableMaxTimeFailPoint()

        when:
        countOperation.execute(getBinding())

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    @Category(Async)
    def 'should throw execution timeout exception from executeAsync'() {
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)))

        given:
        def find = new Find().maxTime(1, SECONDS)
        def countOperation = new CountOperation(getNamespace(), find)
        enableMaxTimeFailPoint()

        when:
        countOperation.executeAsync(getAsyncBinding()).get()

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }
}
