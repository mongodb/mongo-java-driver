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

package com.mongodb.client.model.geojson

import spock.lang.Specification


class PositionSpecification extends Specification {
    def 'constructors should set values'() {
        expect:
        new Position([1.0d, 2.0d]).values == [1.0d, 2.0d]
        new Position(1.0d, 2.0d).values == [1.0d, 2.0d]
        new Position(1.0d, 2.0d, 3.0d).values == [1.0d, 2.0d, 3.0d]
        new Position(1.0d, 2.0d, 3.0d, 4.0d).values == [1.0d, 2.0d, 3.0d, 4.0d]
    }

    def 'constructors should set unmodifiable'() {
        when:
        new Position([1.0d, 2.0d]).values[0] = 3.0d

        then:
        thrown(UnsupportedOperationException)

        when:
        new Position(1.0d, 2.0d).values[0] = 3.0d

        then:
        thrown(UnsupportedOperationException)
    }

    def 'constructor should throw when preconditions are violated'() {
        when:
        new Position(null)

        then:
        thrown(IllegalArgumentException)

        when:
        new Position([1.0])

        then:
        thrown(IllegalArgumentException)

        when:
        new Position([1.0, null])

        then:
        thrown(IllegalArgumentException)
    }

    def 'equals, hashcode and toString should be overridden'() {
        expect:
        new Position(1.0d, 2.0d) == new Position(1.0d, 2.0d)
        new Position(1.0d, 2.0d).hashCode() == new Position(1.0d, 2.0d).hashCode()
        new Position(1.0d, 2.0d).toString() == 'Position{values=[1.0, 2.0]}'
    }
}
