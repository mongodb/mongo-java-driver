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
import org.mongodb.AsyncBlock
import org.mongodb.Document
import org.mongodb.FunctionalSpecification
import org.mongodb.MongoAsyncCursor
import org.mongodb.MongoException
import org.mongodb.connection.SingleResultCallback

import static org.mongodb.Fixture.getAsyncBinding
import static org.mongodb.Fixture.getBinding

class DistinctOperationSpecification extends FunctionalSpecification {

    def 'should be able to distinct by name'() {
        given:
        Document pete = new Document('name', 'Pete').append('age', 38)
        Document sam = new Document('name', 'Sam').append('age', 21)
        Document pete2 = new Document('name', 'Pete').append('age', 25)
        getCollectionHelper().insertDocuments(pete, sam, pete2)

        when:
        DistinctOperation op = new DistinctOperation(getNamespace(), 'name', new Find())
        def result = op.execute(getBinding());

        then:
        List<String> results = result.toList().sort()
        results == ['Pete', 'Sam']
    }

    @Category(Async)
    def 'should be able to distinct by name asynchronously'() {
        given:
        Document pete = new Document('name', 'Pete').append('age', 38)
        Document sam = new Document('name', 'Sam').append('age', 21)
        Document pete2 = new Document('name', 'Pete').append('age', 25)
        getCollectionHelper().insertDocuments(pete, sam, pete2)

        when:
        DistinctOperation op = new DistinctOperation(getNamespace(), 'name', new Find())
        def result = new SingleResultFuture<List<String>>()
        op.executeAsync(getAsyncBinding()).register(new SingleResultCallback<MongoAsyncCursor<String>>() {
            @Override
            void onResult(final MongoAsyncCursor<String> cursor, final MongoException e) {
                cursor.start(new AsyncBlock<String>() {
                    List<Document> docList = []

                    @Override
                    void done() {
                        result.init(docList, null)
                    }

                    @Override
                    void apply(final String value) {
                        if (value != null) {
                            docList += value
                        }
                    }
                })
            }
        })

        then:
        result.get().toList().sort() == ['Pete', 'Sam']
    }

    def 'should be able to distinct by name with find'() {
        given:
        Document pete = new Document('name', 'Pete').append('age', 38)
        Document sam = new Document('name', 'Sam').append('age', 21)
        Document pete2 = new Document('name', 'Pete').append('age', 25)
        getCollectionHelper().insertDocuments(pete, sam, pete2)

        when:
        DistinctOperation op = new DistinctOperation(getNamespace(), 'name', new Find(new Document('age', 25)))
        def result = op.execute(getBinding());

        then:
        List<String> results = result.toList().sort()
        results == ['Pete']
    }

    @Category(Async)
    def 'should be able to distinct by name with find asynchronously'() {
        given:
        Document pete = new Document('name', 'Pete').append('age', 38)
        Document sam = new Document('name', 'Sam').append('age', 21)
        Document pete2 = new Document('name', 'Pete').append('age', 25)
        getCollectionHelper().insertDocuments(pete, sam, pete2)

        when:
        DistinctOperation op = new DistinctOperation(getNamespace(), 'name', new Find(new Document('age', 25)))
        def result = new SingleResultFuture<List<Document>>()
        op.executeAsync(getAsyncBinding()).register(new SingleResultCallback<MongoAsyncCursor<String>>() {
            @Override
            void onResult(final MongoAsyncCursor<String> cursor, final MongoException e) {
                cursor.start(new AsyncBlock<String>() {
                    List<Document> docList = []

                    @Override
                    void done() {
                        result.init(docList, null)
                    }

                    @Override
                    void apply(final String value) {
                        if (value != null) {
                            docList += value
                        }
                    }
                })
            }
        })

        then:
        result.get().toList().sort() == ['Pete']
    }
}
