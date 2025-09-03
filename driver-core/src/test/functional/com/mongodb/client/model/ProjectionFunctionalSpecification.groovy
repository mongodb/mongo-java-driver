/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.client.model


import com.mongodb.MongoQueryException
import com.mongodb.OperationFunctionalSpecification
import org.bson.Document
import org.bson.conversions.Bson

import static com.mongodb.client.model.Filters.and
import static com.mongodb.client.model.Filters.eq
import static com.mongodb.client.model.Filters.text
import static com.mongodb.client.model.Projections.elemMatch
import static com.mongodb.client.model.Projections.exclude
import static com.mongodb.client.model.Projections.excludeId
import static com.mongodb.client.model.Projections.fields
import static com.mongodb.client.model.Projections.include
import static com.mongodb.client.model.Projections.metaTextScore
import static com.mongodb.client.model.Projections.slice

class ProjectionFunctionalSpecification extends OperationFunctionalSpecification {
    def a = new Document('_id', 1).append('x', 'coffee').append('y', [new Document('a', 1).append('b', 2),
                                                               new Document('a', 2).append('b', 3),
                                                               new Document('a', 3).append('b', 4)])
    def aYSlice1 = new Document('_id', 1).append('x', 'coffee').append('y', [new Document('a', 1).append('b', 2)])
    def aYSlice12 = new Document('_id', 1).append('x', 'coffee').append('y', [new Document('a', 2).append('b', 3),
                                                                       new Document('a', 3).append('b', 4)])
    def aNoY = new Document('_id', 1).append('x', 'coffee')
    def aId = new Document('_id', 1)
    def aNoId = new Document().append('x', 'coffee').append('y', [new Document('a', 1).append('b', 2),
                                                           new Document('a', 2).append('b', 3),
                                                           new Document('a', 3).append('b', 4)])
    def aWithScore = new Document('_id', 1).append('x', 'coffee').append('y', [new Document('a', 1).append('b', 2),
                                                                        new Document('a', 2).append('b', 3),
                                                                        new Document('a', 3).append('b', 4)])
                                           .append('score', 1.0)

    def setup() {
        getCollectionHelper().insertDocuments(a)
    }

    def 'find'(Bson projection) {
        getCollectionHelper().find(null, null, projection)
    }

    def 'find'(Bson filter, Bson projection) {
        getCollectionHelper().find(filter, null, projection)
    }

    def 'include'() {
        expect:
        find(include('x')) == [aNoY]
        find(include('x', 'y')) == [a]
        find(include(['x', 'y', 'x'])) == [a]
    }

    def 'exclude'() {
        expect:
        find(exclude('y')) == [aNoY]
        find(exclude('x', 'y')) == [aId]
        find(exclude(['x', 'y', 'x'])) == [aId]
    }

    def 'excludeId helper'() {
        expect:
        find(excludeId()) == [aNoId]
    }

    def 'firstElem'() {
        expect:
        find(new Document('y', new Document('$elemMatch', new Document('a', 1).append('b', 2))),
             fields(include('x'), elemMatch('y'))) == [aYSlice1]
    }

    def 'elemMatch'() {
        expect:
        find(fields(include('x'), elemMatch('y', and(eq('a', 1), eq('b', 2))))) == [aYSlice1]
    }

    def 'slice'() {
        expect:
        find(slice('y', 1)) == [aYSlice1]
        find(slice('y', 1, 2)) == [aYSlice12]
    }

    def 'metaTextScore'() {
        given:
        getCollectionHelper().createIndex(new Document('x', 'text'))

        expect:
        find(text('coffee'), metaTextScore('score')) == [aWithScore]
    }

    def 'combine fields'() {
        expect:
        find(fields(include('x', 'y'), exclude('_id'))) == [aNoId]
    }

    def 'combine fields illegally'() {
        when:
        find(fields(include('x', 'y'), exclude('y'))) == [aNoY]

        then:
        thrown(MongoQueryException)
    }
}
