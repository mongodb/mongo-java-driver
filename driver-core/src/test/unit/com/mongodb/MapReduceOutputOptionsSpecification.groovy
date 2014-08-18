/*
 * Copyright (c) 2008 - 2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb

import com.mongodb.operation.MapReduceOutputOptions as MROO
import spock.lang.Specification

class MapReduceOutputOptionsSpecification extends Specification {

    def 'default options are as expected'() {
        when:
        def mroo = new MROO('test')

        then:
        mroo.getAction() == MROO.Action.REPLACE
        mroo.getCollectionName() == 'test'
        mroo.getDatabaseName() == null
        !mroo.isSharded()
        !mroo.isNonAtomic()
    }

    @SuppressWarnings('ExplicitCallToEqualsMethod')
    def 'test equals'() {
        expect:
        mroo.equals(compareTo) == expectedResult

        where:
        mroo                                         | compareTo                                    | expectedResult
        new MROO('test')                             | new MROO('test')                             | true
        new MROO('test')                             | new MROO('test1')                            | false
        new MROO('test')                             | new MROO('test').database('foo')             | false
        new MROO('test').database('foo')             | new MROO('test').database('foo')             | true
        new MROO('test')                             | new MROO('test').action(MROO.Action.MERGE)   | false
        new MROO('test').action(MROO.Action.MERGE)   | new MROO('test').action(MROO.Action.MERGE)   | true
        new MROO('test').action(MROO.Action.MERGE)   | new MROO('test').action(MROO.Action.REPLACE) | false
        new MROO('test').action(MROO.Action.REPLACE) | new MROO('test').action(MROO.Action.REPLACE) | true
        new MROO('test')                             | new MROO('test').sharded()                   | false
        new MROO('test').sharded()                   | new MROO('test').sharded()                   | true
        new MROO('test')                             | new MROO('test').nonAtomic()                 | false
        new MROO('test').nonAtomic()                 | new MROO('test').nonAtomic()                 | true
        new MROO('test').sharded().nonAtomic()       | new MROO('test').sharded().nonAtomic()       | true
    }

}
