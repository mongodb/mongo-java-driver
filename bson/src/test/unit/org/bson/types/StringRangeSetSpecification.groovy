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

package org.bson.types

import spock.lang.Specification

class StringRangeSetSpecification extends Specification {

    def 'should be empty if size is 0'() {
        when:
        def stringSet = new StringRangeSet(0)

        then:
        stringSet.size() == 0
        stringSet.isEmpty()
    }

    def 'should contain all strings between zero and size'() {
        when:
        def stringSet = new StringRangeSet(5)

        then:
        stringSet.size() == 5
        !stringSet.contains('-1')
        stringSet.contains('0')
        stringSet.contains('1')
        stringSet.contains('2')
        stringSet.contains('3')
        stringSet.contains('4')
        !stringSet.contains('5')
        stringSet.containsAll(['0', '1', '2', '3', '4'])
        !stringSet.containsAll(['0', '1', '2', '3', '4', '5'])
    }

    def 'should not contain integers'() {
        when:
        def stringSet = new StringRangeSet(5)

        then:
        !stringSet.contains(0)
        !stringSet.containsAll([0, 1, 2])
    }

    def 'should not contain strings that do not parse as integers'() {
        when:
        def stringSet = new StringRangeSet(5)

        then:
        !stringSet.contains('foo')
        !stringSet.containsAll(['foo', 'bar', 'baz'])
    }

    def 'set should be ordered string representations of the range'() {
        given:
        def size = 2000;
        def expectedKeys = []
        for (def i : (0..<size)) {
            expectedKeys.add(i.toString())
        }

        when:
        def stringSet = new StringRangeSet(size)

        then:
        def keys = []
        def iter = stringSet.iterator()
        while (iter.hasNext()) {
            keys.add(iter.next())
        }

        then:
        keys == expectedKeys

        when:
        iter.next()

        then:
        thrown(NoSuchElementException)
    }

    def 'should convert to Object array'() {
        given:
        def stringSet = new StringRangeSet(5)

        when:
        def array = stringSet.toArray()

        then:
        array == ['0', '1', '2', '3', '4'] as Object[]
    }

    def 'should modify String array that is large enough'() {
        given:
        def stringSet = new StringRangeSet(5)
        def stringArray = ['6', '5', '4', '3', '2', '1', '0'] as String[]

        when:
        def array = stringSet.toArray(stringArray)

        then:
        array == ['0', '1', '2', '3', '4', null, '0'] as String[]
    }

    def 'should allocate String array when specified one is too small'() {
        given:
        def stringSet = new StringRangeSet(5)
        def stringArray = ['3', '2', '1', '0'] as String[]

        when:
        def array = stringSet.toArray(stringArray)

        then:
        array == ['0', '1', '2', '3', '4'] as String[]
    }

    def 'should throw ArrayStoreException if array is of wrong type'() {
        given:
        def stringSet = new StringRangeSet(5)

        when:
        stringSet.toArray(new Integer[5])

        then:
        thrown(ArrayStoreException)
    }

    def 'modifying operations should throw UnsupportedOperationException'() {
        given:
        def stringSet = new StringRangeSet(5)

        when:
        stringSet.iterator().remove()

        then:
        thrown(UnsupportedOperationException)

        when:
        stringSet.add('1')

        then:
        thrown(UnsupportedOperationException)

        when:
        stringSet.addAll(['1', '2'])

        then:
        thrown(UnsupportedOperationException)

        when:
        stringSet.clear()

        then:
        thrown(UnsupportedOperationException)

        when:
        stringSet.remove('1')

        then:
        thrown(UnsupportedOperationException)

        when:
        stringSet.removeAll(['0', '1'])

        then:
        thrown(UnsupportedOperationException)

        when:
        stringSet.retainAll(['0', '1'])

        then:
        thrown(UnsupportedOperationException)
    }
}
