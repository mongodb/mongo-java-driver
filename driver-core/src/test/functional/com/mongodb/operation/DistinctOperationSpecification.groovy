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
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.SECONDS

class DistinctOperationSpecification extends OperationFunctionalSpecification {

    def 'should be able to distinct by name'() {
        given:
        Document pete = new Document('name', 'Pete').append('age', 38)
        Document sam = new Document('name', 'Sam').append('age', 21)
        Document pete2 = new Document('name', 'Pete').append('age', 25)
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        DistinctOperation op = new DistinctOperation(getNamespace(), 'name')
        def result = op.execute(getBinding());

        then:
        result.toList().sort() == new BsonArray([new BsonString('Pete'), new BsonString('Sam')])
    }

    @Category(Async)
    def 'should be able to distinct by name asynchronously'() {
        given:
        Document pete = new Document('name', 'Pete').append('age', 38)
        Document sam = new Document('name', 'Sam').append('age', 21)
        Document pete2 = new Document('name', 'Pete').append('age', 25)
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        DistinctOperation op = new DistinctOperation(getNamespace(), 'name')
        def result = executeAsync(op)

        then:
        result.sort() == new BsonArray([new BsonString('Pete'), new BsonString('Sam')])
    }

    def 'should be able to distinct by name with find'() {
        given:
        Document pete = new Document('name', 'Pete').append('age', 38)
        Document sam = new Document('name', 'Sam').append('age', 21)
        Document pete2 = new Document('name', 'Pete').append('age', 25)
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        DistinctOperation op = new DistinctOperation(getNamespace(), 'name')
        op.filter(new BsonDocument('age', new BsonInt32(25)))
        def result = op.execute(getBinding());

        then:
        result == new BsonArray([new BsonString('Pete')])
    }

    @Category(Async)
    def 'should be able to distinct by name with find asynchronously'() {
        given:
        Document pete = new Document('name', 'Pete').append('age', 38)
        Document sam = new Document('name', 'Sam').append('age', 21)
        Document pete2 = new Document('name', 'Pete').append('age', 25)
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        DistinctOperation op = new DistinctOperation(getNamespace(), 'name')
        op.filter(new BsonDocument('age', new BsonInt32(25)))
        def result = executeAsync(op)

        then:
        result == new BsonArray([new BsonString('Pete')])
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should throw execution timeout exception from execute'() {
        given:
        def op = new DistinctOperation(getNamespace(), 'name')
        op.maxTime(1, SECONDS)
        enableMaxTimeFailPoint()

        when:
        op.execute(getBinding())

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    @Category(Async)
    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should throw execution timeout exception from executeAsync'() {
        given:
        def op = new DistinctOperation(getNamespace(), 'name')
        op.maxTime(1, SECONDS)
        enableMaxTimeFailPoint()

        when:
        executeAsync(op)

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }
}
