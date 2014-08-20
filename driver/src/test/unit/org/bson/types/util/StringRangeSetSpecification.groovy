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

package org.bson.types.util

import org.bson.util.StringRangeSet
import spock.lang.Specification

import static junit.framework.TestCase.fail

class StringRangeSetSpecification extends Specification {

    def 'should be empty if size is 0'() {
        when:
        def stringSet = new StringRangeSet(0)

        then:
        stringSet.size() == 0
        stringSet.isEmpty()
    }

    def 'should produce the string values'() {
        when:
        def stringSet = new StringRangeSet(5)

        then:
        stringSet.iterator().collect() == ['0', '1', '2', '3', '4']
    }

    def 'should produce the string values in an Array with toArray'() {
        when:
        def stringSet = new StringRangeSet(5)

        then:
        stringSet.toArray() == ['0', '1', '2', '3', '4']
    }

    def 'should implement iterator hasNext() correctly'() {
        when:
        def counter = 0
        def stringSet = new StringRangeSet(5)
        def iter = stringSet.iterator()
        while (iter.hasNext()) {
            if (counter > 5) { fail() }
            counter++
            iter.next()
        }

        then:
        counter == stringSet.size()
    }

    def 'should implement iterator next() correctly'() {
        when:
        def counter = 0
        def iter = new StringRangeSet(5).iterator()
        while (iter.next()) {
            if (counter > 5) { fail() }
            counter++
        }

        then:
        thrown NoSuchElementException
    }

    def 'should provide a valid contains method'() {
        when:
        def stringSet = new StringRangeSet(5)

        then:
        stringSet.contains('3')
        !stringSet.contains('5')
    }

    def 'should provide a valid containsAll method'() {
        when:
        def stringSet = new StringRangeSet(5)

        then:
        stringSet.containsAll(['1', '2', '3', '4'])
        !stringSet.containsAll(['1', '2', '3', '4', '5'])
    }

    @SuppressWarnings('UnnecessaryObjectReferences')
    def 'should throw unsupportedErrors for unsupported functions'() {
        when:
        def stringSet = new StringRangeSet(5)
        stringSet.add('6')

        then:
        thrown UnsupportedOperationException

        when:
        stringSet.addAll(['6', '7'])

        then:
        thrown UnsupportedOperationException


        when:
        stringSet.clear()

        then:
        thrown UnsupportedOperationException


        when:
        stringSet.remove('0')

        then:
        thrown UnsupportedOperationException

        when:
        stringSet.removeAll(['0', '1'])

        then:
        thrown UnsupportedOperationException

        when:
        stringSet.toArray([])

        then:
        thrown UnsupportedOperationException

        when:
        stringSet.iterator().remove()

        then:
        thrown UnsupportedOperationException

    }


}
