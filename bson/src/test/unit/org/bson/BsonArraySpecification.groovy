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

class BsonArraySpecification extends Specification {

    def 'should be array type'() {
        expect:
        new BsonArray().getBsonType() == BsonType.ARRAY
    }

    def 'should construct empty array'() {
        when:
        def array = new BsonArray()

        then:
        array.isEmpty()
        array.size() == 0
        array.getValues().isEmpty()
    }

    def 'should construct from a list'() {
        given:
        def list = [BsonBoolean.TRUE, BsonBoolean.FALSE]

        when:
        def array = new BsonArray(list)

        then:
        !array.isEmpty()
        array.size() == 2
        array.getValues() == list
    }

    def 'should parse json'() {
        expect:
        BsonArray.parse('[1, true]') == new BsonArray([new BsonInt32(1), BsonBoolean.TRUE])
    }
}
