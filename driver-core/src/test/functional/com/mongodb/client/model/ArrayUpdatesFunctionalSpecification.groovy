/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model

import com.mongodb.OperationFunctionalSpecification
import org.bson.Document
import org.bson.conversions.Bson

import static com.mongodb.client.model.Updates.addEachToSet
import static com.mongodb.client.model.Updates.addToSet
import static com.mongodb.client.model.Updates.combine
import static com.mongodb.client.model.Updates.popFirst
import static com.mongodb.client.model.Updates.popLast
import static com.mongodb.client.model.Updates.pull
import static com.mongodb.client.model.Updates.pullAll
import static com.mongodb.client.model.Updates.pullByFilter
import static com.mongodb.client.model.Updates.push
import static com.mongodb.client.model.Updates.pushEach
import static com.mongodb.client.model.Updates.unset

class ArrayUpdatesFunctionalSpecification extends OperationFunctionalSpecification {
    def a = new Document('_id', 1).append('x', [1, 2, 3])


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

    def 'add to set'() {
        when:
        updateOne(addToSet('x', 4))

        then:
        find() == [new Document('_id', 1).append('x', [1, 2, 3, 4])]

        when:
        updateOne(addToSet('x', 4))

        then:
        find() == [new Document('_id', 1).append('x', [1, 2, 3, 4])]

        when:
        updateOne(addEachToSet('x', [4, 5, 6]))

        then:
        find() == [new Document('_id', 1).append('x', [1, 2, 3, 4, 5, 6])]
    }

    def 'push'() {
        when:
        updateOne(push('x', 4))

        then:
        find() == [new Document('_id', 1).append('x', [1, 2, 3, 4])]

        when:
        updateOne(push('x', 4))

        then:
        find() == [new Document('_id', 1).append('x', [1, 2, 3, 4, 4])]

    }

    def 'push with each'() {
        when:
        updateOne(pushEach('x', [4, 4, 4, 5, 6], new PushOptions()))

        then:
        find() == [new Document('_id', 1).append('x', [1, 2, 3, 4, 4, 4, 5, 6])]

        when:
        updateOne(pushEach('x', [4, 5, 6], new PushOptions().position(0).slice(5)))

        then:
        find() == [new Document('_id', 1).append('x', [4, 5, 6, 1, 2])]

        when:
        updateOne(pushEach('x', [], new PushOptions().sort(-1)))

        then:
        find() == [new Document('_id', 1).append('x', [6, 5, 4, 2, 1])]

        when:
        updateOne(combine(unset('x'), pushEach('scores', [new Document('score', 89), new Document('score', 65)],
                                               new PushOptions().sortDocument(new Document('score', 1)))))

        then:
        find() == [new Document('_id', 1).append('scores', [new Document('score', 65), new Document('score', 89)])]
    }

    def 'pull'() {
        when:
        updateOne(pull('x', 1))

        then:
        find() == [new Document('_id', 1).append('x', [2, 3])]
    }

    def 'pullByFilter'() {
        when:
        updateOne(pullByFilter(Filters.gt('x', 1)))

        then:
        find() == [new Document('_id', 1).append('x', [1])]
    }

    def 'pullAll'() {
        when:
        updateOne(pullAll('x', [2, 3]))

        then:
        find() == [new Document('_id', 1).append('x', [1])]
    }

    def 'pop first'() {
        when:
        updateOne(popFirst('x'))

        then:
        find() == [new Document('_id', 1).append('x', [2, 3])]
    }

    def 'pop last'() {
        when:
        updateOne(popLast('x'))

        then:
        find() == [new Document('_id', 1).append('x', [1, 2])]
    }
}
