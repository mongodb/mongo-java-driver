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

package org.mongodb.operation
import category.Async
import org.junit.experimental.categories.Category
import org.mongodb.AggregationOptions
import org.mongodb.AsyncBlock
import org.mongodb.Document
import org.mongodb.FunctionalSpecification
import org.mongodb.MongoAsyncCursor
import org.mongodb.MongoException
import org.mongodb.codecs.DocumentCodec
import org.mongodb.connection.SingleResultCallback

import static org.mongodb.Fixture.getAsyncBinding
import static org.mongodb.Fixture.getBinding

class AggregateOperationSpecification extends FunctionalSpecification {

    def 'should be able to aggregate'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(pete, sam, pete2)

        when:
        AggregateOperation op = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec(), new DocumentCodec(),
                                                                 aggregateOptions)
        def result = op.execute(getBinding());

        then:
        List<String> results = result.iterator()*.getString('name')
        results.size() == 3
        results.containsAll(['Pete', 'Sam'])

        where:
        aggregateOptions << [
                AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.INLINE).build(),
                AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.CURSOR).build()]
    }

    @Category(Async)
    def 'should be able to aggregate asynchronously'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(pete, sam, pete2)

        when:
        AggregateOperation op = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec(), new DocumentCodec(),
                                                                 aggregateOptions)
        def result = new SingleResultFuture<List<Document>>()
        op.executeAsync(getAsyncBinding()).register(new SingleResultCallback<MongoAsyncCursor<Document>>() {
            @Override
            void onResult(final MongoAsyncCursor<Document> cursor, final MongoException e) {
                cursor.start(new AsyncBlock<Document>() {
                    List<Document> docList = []

                    @Override
                    void done() {
                        result.init(docList, null)
                    }

                    @Override
                    void apply(final Document value) {
                        if (value != null) {
                            docList += value
                        }
                    }
                })
            }
        })

        then:
        List<String> results = result.get().iterator()*.getString('name')
        results.size() == 3
        results.containsAll(['Pete', 'Sam'])

        where:
        aggregateOptions << [
                AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.INLINE).build(),
                AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.CURSOR).build()]
    }

    def 'should be able to aggregate with pipeline'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(pete, sam, pete2)

        when:
        AggregateOperation op = new AggregateOperation<Document>(getNamespace(), [new Document('$match', new Document('job', 'plumber'))],
                                                                 new DocumentCodec(), new DocumentCodec(), aggregateOptions)
        def result = op.execute(getBinding());

        then:
        List<String> results = result.iterator()*.getString('name')
        results.size() == 1
        results == ['Sam']

        where:
        aggregateOptions << [
                AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.INLINE).build(),
                AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.CURSOR).build()]
    }

    @Category(Async)
    def 'should be able to aggregate with pipeline asynchronously'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(pete, sam, pete2)

        when:
        AggregateOperation op = new AggregateOperation<Document>(getNamespace(), [new Document('$match', new Document('job', 'plumber'))],
                                                                 new DocumentCodec(), new DocumentCodec(), aggregateOptions)
        def result = new SingleResultFuture<List<Document>>()
        op.executeAsync(getAsyncBinding()).register(new SingleResultCallback<MongoAsyncCursor<Document>>() {
            @Override
            void onResult(final MongoAsyncCursor<Document> cursor, final MongoException e) {
                cursor.start(new AsyncBlock<Document>() {
                    List<Document> docList = []

                    @Override
                    void done() {
                        result.init(docList, null)
                    }

                    @Override
                    void apply(final Document value) {
                        if (value != null) {
                            docList += value
                        }
                    }
                })
            }
        })

        then:
        List<String> results = result.get().iterator()*.getString('name')
        results.size() == 1
        results == ['Sam']

        where:
        aggregateOptions << [
                AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.INLINE).build(),
                AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.CURSOR).build()]
    }
}
