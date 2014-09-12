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
import com.mongodb.Block
import com.mongodb.MongoExecutionTimeoutException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.codecs.DocumentCodec
import org.bson.BsonDocument
import org.bson.BsonString
import org.junit.experimental.categories.Category
import org.mongodb.Document
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.SECONDS

class AggregateOperationSpecification extends OperationFunctionalSpecification {

    def setup() {
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)
    }

    def 'should be able to aggregate'() {
        when:
        AggregateOperation op = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())
        op.setUseCursor(useCursor)
        def result = op.execute(getBinding());

        then:
        List<String> results = result.iterator()*.getString('name')
        results.size() == 3
        results.containsAll(['Pete', 'Sam'])

        where:
        useCursor << useCursorOptions()
    }

    @Category(Async)
    def 'should be able to aggregate asynchronously'() {
        when:
        AggregateOperation op = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())
        op.setUseCursor(useCursor)
        List<Document> docList = []
        def cursor = op.executeAsync(getAsyncBinding()).get(1, SECONDS)
        cursor.forEach(new Block<Document>() {
            @Override
            void apply(final Document value) {
                if (value != null) {
                    docList += value
                }
            }
        }).get(1, SECONDS)

        then:
        List<String> results = docList.iterator()*.getString('name')
        results.size() == 3
        results.containsAll(['Pete', 'Sam'])

        where:
        useCursor << useCursorOptions()
    }

    def 'should be able to aggregate with pipeline'() {
        when:
        AggregateOperation op = new AggregateOperation<Document>(getNamespace(),
                                                                 [new BsonDocument('$match',
                                                                                   new BsonDocument('job', new BsonString('plumber')))],
                                                                 new DocumentCodec())
        op.setUseCursor(useCursor)
        def result = op.execute(getBinding());

        then:
        List<String> results = result.iterator()*.getString('name')
        results.size() == 1
        results == ['Sam']

        where:
        useCursor << useCursorOptions()
    }

    @Category(Async)
    def 'should be able to aggregate with pipeline asynchronously'() {
        when:
        AggregateOperation op = new AggregateOperation<Document>(getNamespace(),
                                                                 [new BsonDocument('$match',
                                                                                   new BsonDocument('job', new BsonString('plumber')))],
                                                                 new DocumentCodec())
        op.setUseCursor(useCursor)
        List<Document> docList = []
        def cursor = op.executeAsync(getAsyncBinding()).get(1, SECONDS)
        cursor.forEach(new Block<Document>() {
            @Override
            void apply(final Document value) {
                if (value != null) {
                    docList += value
                }
            }
        }).get(1, SECONDS)

        then:
        List<String> results = docList.iterator()*.getString('name')
        results.size() == 1
        results == ['Sam']

        where:
        useCursor << useCursorOptions()
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should allow disk usage'() {
        when:
        AggregateOperation op = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())
        op.setAllowDiskUse(allowDiskUse)
        def cursor = op.execute(getBinding())

        then:
        cursor*.getString('name') == ['Pete', 'Sam', 'Pete']

        where:
        allowDiskUse << [null, true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should allow batch size'() {
        when:
        AggregateOperation op = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())
        op.setBatchSize(batchSize)
        def cursor = op.execute(getBinding())

        then:
        cursor*.getString('name') == ['Pete', 'Sam', 'Pete']

        where:
        batchSize << [null, 0, 10]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should throw execution timeout exception from execute'() {
        given:
        def op = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())
        op.setMaxTime(1, SECONDS)
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
        def op = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())

        op.setMaxTime(1, SECONDS)
        enableMaxTimeFailPoint()

        when:
        op.executeAsync(getAsyncBinding()).get()

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    private static List<Boolean> useCursorOptions() {
        [null, true, false]
    }
}
