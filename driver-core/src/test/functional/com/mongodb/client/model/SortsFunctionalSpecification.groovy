/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONObjectITIONS OF ANY KINObject, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model

import com.mongodb.OperationFunctionalSpecification
import org.bson.Document
import org.bson.conversions.Bson
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.client.model.Sorts.ascending
import static com.mongodb.client.model.Sorts.descending
import static com.mongodb.client.model.Sorts.metaTextScore
import static com.mongodb.client.model.Sorts.orderBy

class SortsFunctionalSpecification extends OperationFunctionalSpecification {
    def a = new Document('_id', 1).append('x', 1)
                                  .append('y', 'b')

    def b = new Document('_id', 2).append('x', 1)
                                  .append('y', 'a')

    def c = new Document('_id', 3).append('x', 2)
                                  .append('y', 'c')

    def setup() {
        getCollectionHelper().insertDocuments(a, b, c)
    }

    def 'find'(Bson sort) {
        getCollectionHelper().find(new Document(), sort)
    }

    def 'find'(Bson sort, Bson projection) {
        getCollectionHelper().find(new Document(), sort, projection)
    }

    def 'ascending'() {
        expect:
        find(ascending('_id')) == [a, b, c]
        find(ascending('y')) == [b, a, c]
        find(ascending('x', 'y')) == [b, a, c]
    }

    def 'descending'() {
        expect:
        find(descending('_id')) == [c, b, a]
        find(descending('y')) == [c, a, b]
        find(descending('x', 'y')) == [c, a, b]
    }

    @IgnoreIf({ !serverVersionAtLeast(2, 6) })
    def 'metaTextScore'() {
        given:
        getCollectionHelper().createIndex(new Document('y', 'text'))

        expect:
        find(metaTextScore('score'), new Document('score', new Document('$meta', 'textScore')))*.containsKey('score')
    }

    def 'orderBy'() {
        expect:
        find(orderBy([ascending('x'), descending('y')])) == [a, b, c]
        find(orderBy(ascending('x'), descending('y'), descending('x'))) == [c, a, b]
    }

}