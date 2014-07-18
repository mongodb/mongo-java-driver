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
import org.bson.BsonDocument
import org.bson.BsonString
import org.junit.experimental.categories.Category
import org.mongodb.AggregationOptions
import org.mongodb.Document
import org.mongodb.FunctionalSpecification
import spock.lang.Shared

import static java.util.Arrays.asList
import static org.junit.Assume.assumeTrue
import static org.mongodb.Fixture.getAsyncBinding
import static org.mongodb.Fixture.getBinding
import static org.mongodb.Fixture.getDefaultDatabase
import static org.mongodb.Fixture.initialiseCollection
import static org.mongodb.Fixture.serverVersionAtLeast

class AggregateToCollectionOperationSpecification extends FunctionalSpecification {

    @Shared
            outCollection

    def setup() {
        outCollection = initialiseCollection(getDefaultDatabase(), 'aggregateCollection')
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(pete, sam, pete2)
    }

    def 'should not accept an empty pipeline'() {
        when:
        new AggregateToCollectionOperation(getNamespace(), [], AggregationOptions.builder().build())


        then:
        thrown(IllegalArgumentException)
    }

    def 'should not accept a pipeline without the last stage specifying an output-collection'() {
        when:
        new AggregateToCollectionOperation(getNamespace(), [new BsonDocument('$match',
                                                                             new BsonDocument('job', new BsonString('plumber')))],
                                           AggregationOptions.builder().build())


        then:
        thrown(IllegalArgumentException)
    }

    def 'should be able to output to a collection'() {
        assumeTrue(serverVersionAtLeast(asList(2, 6, 0)))

        when:
        AggregateToCollectionOperation op =
                new AggregateToCollectionOperation(getNamespace(),
                                                   [new BsonDocument('$out', new BsonString(outCollection.name))],
                                                   AggregationOptions.builder().build())
        op.execute(getBinding());

        then:
        getCollectionHelper(outCollection.namespace).count() == 3
    }

    @Category(Async)
    def 'should be able to output to a collection asynchronously'() {
        assumeTrue(serverVersionAtLeast(asList(2, 6, 0)))

        when:
        AggregateToCollectionOperation op =
                new AggregateToCollectionOperation(getNamespace(),
                                                   [new BsonDocument('$out', new BsonString(outCollection.name))],
                                                   AggregationOptions.builder().build())
        op.executeAsync(getAsyncBinding()).get();

        then:
        getCollectionHelper(outCollection.namespace).count() == 3
    }

    def 'should be able to match then output to a collection'() {
        assumeTrue(serverVersionAtLeast(asList(2, 6, 0)))

        when:
        AggregateToCollectionOperation op =
                new AggregateToCollectionOperation(getNamespace(),
                                                   [new BsonDocument('$match', new BsonDocument('job', new BsonString('plumber'))),
                                                    new BsonDocument('$out', new BsonString(outCollection.name))],
                                                   AggregationOptions.builder().build())
        op.execute(getBinding());

        then:
        getCollectionHelper(outCollection.namespace).count() == 1
    }

    @Category(Async)
    def 'should be able to match then output to a collection asynchronously'() {
        assumeTrue(serverVersionAtLeast(asList(2, 6, 0)))

        when:
        AggregateToCollectionOperation op =
                new AggregateToCollectionOperation(getNamespace(),
                                                   [new BsonDocument('$match', new BsonDocument('job', new BsonString('plumber'))),
                                                    new BsonDocument('$out', new BsonString(outCollection.name))],
                                                   AggregationOptions.builder().build())
        op.executeAsync(getAsyncBinding()).get();

        then:
        getCollectionHelper(outCollection.namespace).count() == 1
    }

}
