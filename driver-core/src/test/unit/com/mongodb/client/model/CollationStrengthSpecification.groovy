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

import spock.lang.Specification

class CollationStrengthSpecification extends Specification {

    def 'should return the expected string value'() {
        expect:
        collationStrength.getIntRepresentation() == expectedInt

        where:
        collationStrength                  | expectedInt
        CollationStrength.PRIMARY          | 1
        CollationStrength.SECONDARY        | 2
        CollationStrength.TERTIARY         | 3
        CollationStrength.QUATERNARY       | 4
        CollationStrength.IDENTICAL        | 5
    }

    def 'should support valid string representations'() {
        expect:
        CollationStrength.fromInt(intValue) == collationStrength

        where:
        collationStrength                  | intValue
        CollationStrength.PRIMARY          | 1
        CollationStrength.SECONDARY        | 2
        CollationStrength.TERTIARY         | 3
        CollationStrength.QUATERNARY       | 4
        CollationStrength.IDENTICAL        | 5
    }

    def 'should throw an illegal Argument exception for invalid values'() {
        when:
        CollationStrength.fromInt(intValue)

        then:
        thrown(IllegalArgumentException)

        where:
        intValue << [0, 6]
    }
}
