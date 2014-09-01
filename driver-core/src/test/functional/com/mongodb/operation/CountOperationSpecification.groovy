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
import com.mongodb.MongoException
import com.mongodb.MongoExecutionTimeoutException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.codecs.DocumentCodec
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.junit.experimental.categories.Category
import org.mongodb.Document
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.operation.OrderBy.ASC
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.SECONDS

class CountOperationSpecification extends OperationFunctionalSpecification {

    private List<InsertRequest<Document>> insertDocumentList;

    def findWithBadHint = new Find( new BsonDocument('a', new BsonInt32(1))).hintIndex('BAD HINT')

    def setup() {
        insertDocumentList = [
                new InsertRequest<Document>(new Document('x', 1)),
                new InsertRequest<Document>(new Document('x', 2)),
                new InsertRequest<Document>(new Document('x', 3)),
                new InsertRequest<Document>(new Document('x', 4)),
                new InsertRequest<Document>(new Document('x', 5))
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

    @IgnoreIf( { !serverVersionAtLeast(asList(2, 6, 0)) } )
    def 'should throw execution timeout exception from execute'() {
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
    @IgnoreIf( { !serverVersionAtLeast(asList(2, 6, 0)) } )
    def 'should throw execution timeout exception from executeAsync'() {
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

    def 'should use limit with the count'() {
        when:
        def find = new Find().limit(1)
        def countOperation = new CountOperation(getNamespace(), find)

        then:
        countOperation.execute(getBinding()) == 1
    }

    @Category(Async)
    def 'should use limit with the count asynchronously'() {
        when:
        def find = new Find().limit(1)
        def countOperation = new CountOperation(getNamespace(), find)

        then:
        countOperation.executeAsync(getAsyncBinding()).get() == 1
    }

    def 'should use skip with the count'() {
        when:
        def find = new Find().skip(insertDocumentList.size() - 2)
        def countOperation = new CountOperation(getNamespace(), find)

        then:
        countOperation.execute(getBinding()) == 2
    }

    @Category(Async)
    def 'should use skip with the count asynchronously'() {
        when:
        def find = new Find().skip(insertDocumentList.size() - 2)
        def countOperation = new CountOperation(getNamespace(), find)

        then:
        countOperation.executeAsync(getAsyncBinding()).get() == 2
    }

    def 'should use hint with the count'() {
        given:
        def index = Index.builder().addKey('x', ASC).sparse().build()
        def createIndexesOperation = new CreateIndexesOperation(getNamespace(), [index])
        def find = new Find().hintIndex('x_1')
        def countOperation = new CountOperation(getNamespace(), find)

        when:
        createIndexesOperation.execute(getBinding())

        then:
        countOperation.execute(getBinding()) == serverVersionAtLeast(asList(2, 6, 0)) ? 1 : insertDocumentList.size()
    }

    @Category(Async)
    def 'should use hint with the count asynchronously'() {
        given:
        def index = Index.builder().addKey('x', ASC).sparse().build()
        def createIndexesOperation = new CreateIndexesOperation(getNamespace(), [index])
        def find = new Find().hintIndex('x_1')
        def countOperation = new CountOperation(getNamespace(), find)

        when:
        createIndexesOperation.executeAsync(getAsyncBinding()).get()

        then:
        countOperation.executeAsync(getAsyncBinding()).get() == serverVersionAtLeast(asList(2, 6, 0)) ? 1 : insertDocumentList.size()
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should throw with bad hint with mongod 2.6+'() {
        given:
        def countOperation = new CountOperation(getNamespace(), findWithBadHint)

        when:
        countOperation.execute(getBinding())

        then:
        thrown(MongoException)
    }

    @Category(Async)
    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should throw with bad hint with mongod 2.6+ asynchronously'() {
        given:
        def countOperation = new CountOperation(getNamespace(), findWithBadHint)

        when:
        countOperation.executeAsync(getAsyncBinding()).get()

        then:
        thrown(MongoException)
    }

    @IgnoreIf({ serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should ignore with bad hint with mongod < 2.6'() {
        given:
        def countOperation = new CountOperation(getNamespace(), findWithBadHint)

        when:
        countOperation.execute(getBinding())

        then:
        notThrown(MongoException)
    }

    @Category(Async)
    @IgnoreIf({ serverVersionAtLeast(asList(2, 6, 0))} )
    def 'should ignore with bad hint with mongod < 2.6 asynchronously'() {
        given:
        def countOperation = new CountOperation(getNamespace(), findWithBadHint)

        when:
        countOperation.executeAsync(getAsyncBinding()).get()

        then:
        notThrown(MongoException)
    }
}
