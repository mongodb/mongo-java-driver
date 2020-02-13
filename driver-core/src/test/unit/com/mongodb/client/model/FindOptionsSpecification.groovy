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

import com.mongodb.CursorType
import com.mongodb.internal.client.model.FindOptions
import org.bson.BsonDocument
import org.bson.Document
import spock.lang.Specification

import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS

class FindOptionsSpecification extends Specification {

    def 'should have the expected defaults'() {
        when:
        def options = new FindOptions()

        then:
        options.getCollation() == null
        options.getMaxTime(MILLISECONDS) == 0
        options.getMaxAwaitTime(MILLISECONDS) == 0
        options.getProjection() == null
        options.getSort() == null
        options.getHint() == null
        options.getHintString() == null
        options.getLimit() == 0
        options.getSkip() == 0
        options.getBatchSize() == 0
        options.getCursorType() == CursorType.NonTailable
        !options.isNoCursorTimeout()
        !options.isOplogReplay()
        !options.isPartial()
        !options.isAllowDiskUse()
    }

    def 'should set collation'() {
        expect:
        new FindOptions().collation(collation).getCollation() == collation

        where:
        collation << [null, Collation.builder().locale('en').build()]
    }

    def 'should set projection'() {
        expect:
        new FindOptions().projection(projection).getProjection() == projection

        where:
        projection << [null, BsonDocument.parse('{a: 1}')]
    }

    def 'should set sort'() {
        expect:
        new FindOptions().sort(sort).getSort() == sort

        where:
        sort << [null, BsonDocument.parse('{a: 1}')]
    }

    def 'should set limit'() {
        expect:
        new FindOptions().limit(limit).getLimit() == limit

        where:
        limit << [-1, 0, 1]
    }

    def 'should set skip'() {
        expect:
        new FindOptions().skip(skip).getSkip() == skip

        where:
        skip << [-1, 0, 1]
    }

    def 'should set batchSize'() {
        expect:
        new FindOptions().batchSize(batchSize).getBatchSize() == batchSize

        where:
        batchSize << [-1, 0, 1]
    }

    def 'should set cursorType'() {
        expect:
        new FindOptions().cursorType(cursorType).getCursorType() == cursorType

        where:
        cursorType << [CursorType.NonTailable, CursorType.TailableAwait, CursorType.Tailable]
    }

    def 'should set partial'() {
        expect:
        new FindOptions().partial(partial).isPartial() == partial

        where:
        partial << [true, false]
    }

    def 'should set oplogReplay'() {
        expect:
        new FindOptions().oplogReplay(oplogReplay).isOplogReplay() == oplogReplay

        where:
        oplogReplay << [true, false]
    }

    def 'should set noCursorTimeout'() {
        expect:
        new FindOptions().noCursorTimeout(noCursorTimeout).isNoCursorTimeout() == noCursorTimeout

        where:
        noCursorTimeout << [true, false]
    }

    def 'should convert maxTime'() {
        when:
        def options = new FindOptions()

        then:
        options.getMaxTime(SECONDS) == 0

        when:
        options.maxTime(100, MILLISECONDS)

        then:
        options.getMaxTime(MILLISECONDS) == 100

        when:
        options.maxTime(1004, MILLISECONDS)

        then:
        options.getMaxTime(SECONDS) == 1
    }

    def 'should convert maxAwaitTime'() {
        when:
        def options = new FindOptions()

        then:
        options.getMaxAwaitTime(SECONDS) == 0

        when:
        options.maxAwaitTime(100, MILLISECONDS)

        then:
        options.getMaxAwaitTime(MILLISECONDS) == 100

        when:
        options.maxAwaitTime(1004, MILLISECONDS)

        then:
        options.getMaxAwaitTime(SECONDS) == 1
    }

    def 'should set hint'() {
        expect:
        new FindOptions().hint(hint).getHint() == hint

        where:
        hint << [null, new BsonDocument(), new Document('a', 1)]
    }

    def 'should set hintString'() {
        expect:
        new FindOptions().hintString(hintString).getHintString() == hintString

        where:
        hintString << [null, 'a_1']
    }

    def 'should set allowDiskUse'() {
        expect:
        new FindOptions().allowDiskUse(allowDiskUse).isAllowDiskUse() == allowDiskUse

        where:
        allowDiskUse << [true, false]
    }
}
