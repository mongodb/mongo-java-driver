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

import com.mongodb.OperationFunctionalSpecification
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.conversions.Bson

import static com.mongodb.client.model.Updates.combine
import static com.mongodb.client.model.Updates.currentDate
import static com.mongodb.client.model.Updates.currentTimestamp
import static com.mongodb.client.model.Updates.inc
import static com.mongodb.client.model.Updates.max
import static com.mongodb.client.model.Updates.min
import static com.mongodb.client.model.Updates.mul
import static com.mongodb.client.model.Updates.rename
import static com.mongodb.client.model.Updates.set
import static com.mongodb.client.model.Updates.setOnInsert
import static com.mongodb.client.model.Updates.unset

class UpdatesFunctionalSpecification extends OperationFunctionalSpecification {
    def a = new Document('_id', 1).append('x', 1)


    def setup() {
        getCollectionHelper().insertDocuments(a)
    }

    def find() {
        find(new Document('_id', 1))
    }

    def find(Bson filter) {
        getCollectionHelper().find(filter)
    }

    def updateOne(Bson update) {
        getCollectionHelper().updateOne(new Document('_id', 1), update)
    }

    def updateOne(Bson filter, Bson update, boolean isUpsert) {
        getCollectionHelper().updateOne(filter, update, isUpsert)
    }

    def 'set'() {
        when:
        updateOne(set('x', 5))

        then:
        find() == [new Document('_id', 1).append('x', 5)]
    }

    def 'setOnInsert'() {
        when:
        updateOne(setOnInsert('y', 5))

        then:
        find() == [a]

        when:
        updateOne(new Document('_id', 2), setOnInsert('y', 5), true)

        then:
        find(new Document('_id', 2)) == [new Document('_id', 2).append('y', 5)]

        when:
        updateOne(new Document('_id', 3), setOnInsert(Document.parse('{a: 1, b: "two"}')), true)

        then:
        find(new Document('_id', 3)) == [Document.parse('{_id: 3, a: 1, b: "two"}')]
    }

    def 'unset'() {
        when:
        updateOne(unset('x'))

        then:
        find() == [new Document('_id', 1)]
    }

    def 'rename'() {
        when:
        updateOne(rename('x', 'y'))

        then:
        find() == [new Document('_id', 1).append('y', 1)]
    }

    def 'inc'() {
        when:
        updateOne(inc('x', 5))

        then:
        find() == [new Document('_id', 1).append('x', 6)]

        when:
        updateOne(inc('x', 5L))

        then:
        find() == [new Document('_id', 1).append('x', 11)]

        when:
        updateOne(inc('x', 3.4d))

        then:
        find() == [new Document('_id', 1).append('x', 14.4)]
    }

    def 'mul'() {
        when:
        updateOne(mul('x', 5))

        then:
        find() == [new Document('_id', 1).append('x', 5)]

        when:
        updateOne(mul('x', 5L))

        then:
        find() == [new Document('_id', 1).append('x', 25)]

        when:
        updateOne(mul('x', 3.5d))

        then:
        find() == [new Document('_id', 1).append('x', 87.5)]
    }

    def 'min'() {
        when:
        updateOne(min('x', -1))

        then:
        find() == [new Document('_id', 1).append('x', -1)]
    }

    def 'max'() {
        when:
        updateOne(max('x', 5))

        then:
        find() == [new Document('_id', 1).append('x', 5)]
    }

    def 'currentDate'() {
        when:
        updateOne(currentDate('y'))

        then:
        find()[0].get('y').getClass() == Date

        when:
        updateOne(currentTimestamp('z'))

        then:
        find()[0].get('z').getClass() == BsonTimestamp
    }

    def 'combine single operator'() {
        when:
        updateOne(combine(set('x', 5), set('y', 6)))

        then:
        find() == [new Document('_id', 1).append('x', 5).append('y', 6)]
    }

    def 'combine multiple operators'() {
        when:
        updateOne(combine(set('a', 5), set('b', 6), inc('x', 3), inc('y', 5)))

        then:
        find() == [new Document('_id', 1).append('a', 5).append('b', 6).append('x', 4).append('y', 5)]
    }
}
