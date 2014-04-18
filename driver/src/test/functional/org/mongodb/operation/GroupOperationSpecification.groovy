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
import org.bson.types.Code
import org.junit.experimental.categories.Category
import org.mongodb.AsyncBlock
import org.mongodb.Document
import org.mongodb.FunctionalSpecification
import org.mongodb.MongoAsyncCursor
import org.mongodb.MongoException
import org.mongodb.connection.SingleResultCallback

import static org.mongodb.Fixture.getAsyncBinding
import static org.mongodb.Fixture.getBinding

class GroupOperationSpecification extends FunctionalSpecification {

    def 'should be able to group by name'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(pete, sam, pete2)

        Group group = new Group(new Document('name',  1), new Code('function ( curr, result ) {}'), new Document())

        when:
        GroupOperation op = new GroupOperation(getNamespace(), group)
        def result = op.execute(getBinding());

        then:
        List<String> results = result.iterator()*.getString('name')
        results.containsAll(['Pete', 'Sam'])
    }

    @Category(Async)
    def 'should be able to group by name asynchronously'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(pete, sam, pete2)

        Group group = new Group(new Document('name',  1), new Code('function ( curr, result ) {}'), new Document())

        when:
        GroupOperation op = new GroupOperation(getNamespace(), group)
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
        result.get().iterator()*.getString('name') containsAll(['Pete', 'Sam'])
    }
}
