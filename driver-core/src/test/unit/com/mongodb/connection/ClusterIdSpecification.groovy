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

import spock.lang.Specification
import spock.lang.Unroll


class ClusterIdSpecification extends Specification {
    def 'should set value to string with length 24'() {
        expect:
        new ClusterId().value.length() == 24
    }

    def 'different ids should have different values'() {
        expect:
        new ClusterId().value != new ClusterId().value
    }

    @Unroll
    def 'equivalent ids should be equal and have same hash code'() {
        when:
        def id1 = new ClusterId(id, description)
        def id2 = new ClusterId(id, description)

        then:
        id1 == id2
        id1.hashCode() == id2.hashCode()

        where:
          id  | description
        'id1' | null
        'id2' | 'my server'
    }

}
