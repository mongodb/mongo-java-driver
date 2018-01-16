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

    def 'should throw on unmapped index'() {
        when:
        indexMap.map(-1)

        then:
        thrown(MongoInternalException)

        when:
        indexMap.map(4)

        then:
        thrown(MongoInternalException)

        where:
        indexMap << [IndexMap.create().add(0, 1), IndexMap.create(1000, 3).add(5, 1005)]
    }

    def 'should map indexes when count is provided up front'() {
        when:
        def indexMap = IndexMap.create(1, 2)

        then:
        1 == indexMap.map(0)
        2 == indexMap.map(1)
    }

    def 'should include ranges when converting from range based to hash based indexMap'() {
        given:
        def indexMap = IndexMap.create(1000, 3)

        when: 'converts from range based with a high startIndex to hash based'
        indexMap = indexMap.add(5, 1005)

        then:
        1000 == indexMap.map(0)
        1001 == indexMap.map(1)
        1002 == indexMap.map(2)
        1005 == indexMap.map(5)
    }

    def 'should not allow a negative startIndex or count'() {
        when:
        IndexMap.create(-1, 10)

        then:
        thrown(IllegalArgumentException)

        when:
        IndexMap.create(1, -10)

        then:
        thrown(IllegalArgumentException)
    }

}
