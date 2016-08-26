/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model

import com.mongodb.BasicDBObject
import com.mongodb.CursorType
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import spock.lang.Specification

import java.util.concurrent.TimeUnit

class DBCollectionFindOptionsSpecification extends Specification {

    def 'should have the expected default values'() {
        when:
        def options = new DBCollectionFindOptions()

        then:
        !options.isNoCursorTimeout()
        !options.isOplogReplay()
        !options.isPartial()
        options.getBatchSize() == 0
        options.getCollation() == null
        options.getCursorType() == CursorType.NonTailable
        options.getLimit() == 0
        options.getMaxAwaitTime(TimeUnit.MILLISECONDS) == 0
        options.getMaxTime(TimeUnit.MILLISECONDS) == 0
        options.getModifiers() == new BasicDBObject()
        options.getProjection() == null
        options.getReadConcern() == null
        options.getReadPreference() == null
        options.getSkip() == 0
        options.getSort() == null
    }

    def 'should set and return the expected values'() {
        given:
        def collation = Collation.builder().locale('en').build()
        def projection = BasicDBObject.parse('{a: 1, _id: 0}')
        def sort = BasicDBObject.parse('{a: 1}')
        def cursorType = CursorType.TailableAwait
        def modifiers = BasicDBObject.parse('{$comment: 1}')
        def readConcern = ReadConcern.LOCAL
        def readPreference = ReadPreference.nearest()

        when:
        def options = new DBCollectionFindOptions()
                .batchSize(1)
                .collation(collation)
                .cursorType(cursorType)
                .limit(1)
                .maxAwaitTime(1, TimeUnit.MILLISECONDS)
                .maxTime(1, TimeUnit.MILLISECONDS)
                .modifiers(modifiers)
                .noCursorTimeout(true)
                .oplogReplay(true)
                .partial(true)
                .projection(projection)
                .readConcern(readConcern)
                .readPreference(readPreference)
                .skip(1)
                .sort(sort)

        then:
        options.getBatchSize() == 1
        options.getCollation() == collation
        options.getCursorType() == cursorType
        options.getLimit() == 1
        options.getMaxAwaitTime(TimeUnit.MILLISECONDS) == 1
        options.getMaxTime(TimeUnit.MILLISECONDS) == 1
        options.getModifiers() == modifiers
        options.getProjection() == projection
        options.getReadConcern() == readConcern
        options.getReadPreference() == readPreference
        options.getSkip() == 1
        options.getSort() == sort
        options.isNoCursorTimeout()
        options.isOplogReplay()
        options.isPartial()
    }

    def 'it should copy and return the expected values'() {
        given:
        def collation = Collation.builder().locale('en').build()
        def projection = BasicDBObject.parse('{a: 1, _id: 0}')
        def sort = BasicDBObject.parse('{a: 1}')
        def cursorType = CursorType.TailableAwait
        def modifiers = BasicDBObject.parse('{$comment: 1}')
        def readConcern = ReadConcern.LOCAL
        def readPreference = ReadPreference.nearest()

        when:
        def original = new DBCollectionFindOptions()
                .batchSize(1)
                .collation(collation)
                .cursorType(cursorType)
                .limit(1)
                .maxAwaitTime(1, TimeUnit.MILLISECONDS)
                .maxTime(1, TimeUnit.MILLISECONDS)
                .modifiers(modifiers)
                .noCursorTimeout(true)
                .oplogReplay(true)
                .partial(true)
                .projection(projection)
                .readConcern(readConcern)
                .readPreference(readPreference)
                .skip(1)
                .sort(sort)

        def options = original.copy()

        then:
        original != options

        options.getBatchSize() == 1
        options.getCollation() == collation
        options.getCursorType() == cursorType
        options.getLimit() == 1
        options.getMaxAwaitTime(TimeUnit.MILLISECONDS) == 1
        options.getMaxTime(TimeUnit.MILLISECONDS) == 1
        options.getModifiers() == modifiers
        options.getProjection() == projection
        options.getReadConcern() == readConcern
        options.getReadPreference() == readPreference
        options.getSkip() == 1
        options.getSort() == sort
        options.isNoCursorTimeout()
        options.isOplogReplay()
        options.isPartial()
    }

}
