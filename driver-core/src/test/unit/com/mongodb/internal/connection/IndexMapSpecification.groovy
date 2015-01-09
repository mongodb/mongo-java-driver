/*
 * Copyright (c) 2008 - 2014 MongoDB Inc. <http://mongodb.com>
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

package com.mongodb.internal.connection

import com.mongodb.MongoInternalException
import spock.lang.Specification

class IndexMapSpecification extends Specification {

    def 'should map contiguous indexes'() {
        given:
        def indexMap = IndexMap.create()

        when:
        indexMap = indexMap.add(0, 1)
        indexMap = indexMap.add(1, 2)

        then:
        1 == indexMap.map(0)
        2 == indexMap.map(1)
    }

    def 'should throw on unmapped contiguous index'() {
        given:
        def indexMap = IndexMap.create()
        indexMap.add(0, 1)
        when:
        indexMap.map(3)

        then:
        thrown(MongoInternalException)
    }

    def 'should map non-contiguous indexes'() {
        given:
        def indexMap = IndexMap.create()

        when:
        indexMap = indexMap.add(0, 1)
        indexMap = indexMap.add(1, 2)
        indexMap = indexMap.add(2, 5)

        then:
        1 == indexMap.map(0)
        2 == indexMap.map(1)
        5 == indexMap.map(2)
    }

    def 'should throw on unmapped non-contiguous index'() {
        given:
        def indexMap = IndexMap.create()
        indexMap = indexMap.add(0, 1)
        indexMap = indexMap.add(3, 5)

        when:
        indexMap.map(7)

        then:
        thrown(MongoInternalException)
    }

    def 'should map indexes when count is provided up front'() {
        when:
        def indexMap = IndexMap.create(1, 2)

        then:
        1 == indexMap.map(0)
        2 == indexMap.map(1)
    }
}