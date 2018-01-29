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

package org.bson

import spock.lang.Specification

class BsonRegularExpressionSpecification extends Specification {
    def 'should get type'() {
        expect:
        new BsonRegularExpression('abc', '').bsonType == BsonType.REGULAR_EXPRESSION
    }

    def 'should sort options'() {
        expect:
        new BsonRegularExpression('abc', 'uxsmi').options == 'imsux'
    }

    def 'should accept invalid options'() {
        expect:
        new BsonRegularExpression('abc', 'uxsmiw').options == 'imsuwx'
    }

    def 'should allow null options'() {
        expect:
        new BsonRegularExpression('abc').options == ''
        new BsonRegularExpression('abc', null).options == ''
    }

    def 'should get regular expression'() {
        expect:
        new BsonRegularExpression('abc', null).pattern == 'abc'
    }

    def 'equivalent values should be equal and have same hashcode'() {
        given:
        def first = new BsonRegularExpression('abc', 'uxsmi')
        def second = new BsonRegularExpression('abc', 'imsxu')

        expect:
        first.equals(second)
        first.hashCode() == second.hashCode()
    }

    def 'should convert to string'() {
        expect:
        new BsonRegularExpression('abc', 'uxsmi').toString() == 'BsonRegularExpression{pattern=\'abc\', options=\'imsux\'}'
    }
}
