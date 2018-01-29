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

class CollationMaxVariableSpecification extends Specification {

    def 'should return the expected string value'() {
        expect:
        collationMaxVariable.getValue() == expectedString

        where:
        collationMaxVariable          | expectedString
        CollationMaxVariable.PUNCT    | 'punct'
        CollationMaxVariable.SPACE    | 'space'
    }

    def 'should support valid string representations'() {
        expect:
        CollationMaxVariable.fromString(stringValue) == collationMaxVariable

        where:
        collationMaxVariable          | stringValue
        CollationMaxVariable.PUNCT    | 'punct'
        CollationMaxVariable.SPACE    | 'space'
    }

    def 'should throw an illegal Argument exception for invalid values'() {
        when:
        CollationMaxVariable.fromString(stringValue)

        then:
        thrown(IllegalArgumentException)

        where:
        stringValue << [null, 'info']
    }
}
