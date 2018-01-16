/*
 * Copyright (c) 2008-2016 MongoDB, Inc.
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

package org.bson.types

import org.bson.BSONObject
import spock.lang.Specification

class BasicBSONListSpecification extends Specification {

    def 'should support int keys'() {
        when:
        BSONObject obj = new BasicBSONList()
        obj.put(0, 'a')
        obj.put(1, 'b')
        obj.put(2, 'c')

        then:
        obj == ['a', 'b', 'c'] as BasicBSONList
    }

    def 'should support keys that are strings which be converted to ints'() {
        when:
        BSONObject obj = new BasicBSONList()
        obj.put('0', 'a')
        obj.put('1', 'b')
        obj.put('2', 'c')

        then:
        obj == ['a', 'b', 'c'] as BasicBSONList
    }

    def 'should throw IllegalArgumentException if passed invalid string key'() {
        when:
        BSONObject obj = new BasicBSONList()
        obj.put('ZERO', 'a')

        then:
        thrown IllegalArgumentException
    }

    def 'should insert null values for missing keys'() {
        when:
        BSONObject obj = new BasicBSONList()
        obj.put(0, 'a')
        obj.put(1, 'b')
        obj.put(5, 'c')

        then:
        obj == ['a', 'b', null, null, null, 'c'] as BasicBSONList
    }

    def 'should provide an iterable keySet'() {
        when:
        BSONObject obj = new BasicBSONList()
        obj.put(0, 'a')
        obj.put(1, 'b')
        obj.put(5, 'c')
        def iter = obj.keySet().iterator()

        then:
        iter.hasNext()
        iter.next() == '0'
        iter.hasNext()
        iter.next() == '1'
        iter.hasNext()
        iter.next() == '2'
        iter.hasNext()
        iter.next() == '3'
        iter.hasNext()
        iter.next() == '4'
        iter.hasNext()
        iter.next() == '5'
        !iter.hasNext()
    }
}
