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

package org.bson.types

import spock.lang.Specification

class StringRangeSetSpecification extends Specification {

    def 'should be empty if size is 0'() {
        when:
        def stringSet = new StringRangeSet(0)

        then:
        stringSet.getSet().size() == 0
        stringSet.getSet().isEmpty()
    }

    def 'should produce the string values of the numbers between zero and size'() {
        when:
        def stringSet = new StringRangeSet(5)

        then:
        stringSet.getSet().size() == 5
        stringSet.getSet().containsAll(['0', '1', '2', '3', '4'])
    }

    def 'set should be ordered'() {
        when:
        def counter = 0
        def stringSet = new StringRangeSet(5)

        then:
        def iter = stringSet.getSet().iterator() // work around for codenarc bug
        while (iter.hasNext()) {
            iter.next() == counter.toString()
            counter++
        }

        then:
        counter == 5
    }
}
