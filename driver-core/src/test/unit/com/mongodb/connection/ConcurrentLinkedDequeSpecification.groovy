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

package com.mongodb.connection

import com.mongodb.internal.connection.ConcurrentLinkedDeque
import spock.lang.Specification

// Mostly untested since it was a straight copy from an existing high-quality open source implementation
class ConcurrentLinkedDequeSpecification extends Specification {
    def 'should report successful removal from iterator'() {
        given:
        def deque = new ConcurrentLinkedDeque<Integer>()
        deque.add(1)
        deque.add(2)
        deque.add(3)
        def iter = deque.iterator()

        when:
        def next = iter.next()
        def successfullyRemoved = iter.reportingRemove()

        then:
        next == 1
        successfullyRemoved

        when:
        next = iter.next()
        successfullyRemoved = iter.reportingRemove()

        then:
        next == 2
        successfullyRemoved

        when:
        next = iter.next()
        successfullyRemoved = iter.reportingRemove()

        then:
        next == 3
        successfullyRemoved
    }

    def 'should report unsuccessful removal from iterator'() {
        given:
        def deque = new ConcurrentLinkedDeque<Integer>()
        deque.add(1)
        deque.add(2)
        deque.add(3)
        def iter = deque.iterator()

        when:
        def next = iter.next()
        deque.remove(next)
        def successfullyRemoved = iter.reportingRemove()

        then:
        next == 1
        !successfullyRemoved

        when:
        next = iter.next()
        deque.remove(next)
        successfullyRemoved = iter.reportingRemove()

        then:
        next == 2
        !successfullyRemoved

        when:
        next = iter.next()
        deque.remove(next)
        successfullyRemoved = iter.reportingRemove()

        then:
        next == 3
        !successfullyRemoved
    }

}
