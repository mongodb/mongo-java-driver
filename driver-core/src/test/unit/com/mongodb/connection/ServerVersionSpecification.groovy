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

class ServerVersionSpecification extends Specification {

    def 'should default to version zero'() {
        when:
        def version = new ServerVersion()


        then:
        version.versionList == [0, 0, 0]
    }

    def 'should not accept null version array'() {
        when:
        new ServerVersion(null)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should not accept version array of length unequal to three'() {
        when:
        new ServerVersion([2, 5, 1, 0])

        then:
        thrown(IllegalStateException)

        when:
        new ServerVersion([2, 5])

        then:
        thrown(IllegalStateException)
    }

    def 'should have same version array as when constructed'() {
        when:
        def version = new ServerVersion([3, 4, 1])

        then:
        version.versionList == [3, 4, 1]
    }

    def 'should have immutable version array'() {
        given:
        def version = new ServerVersion([3, 4, 1])

        when:
        version.versionList[0] = 1

        then:
        thrown(UnsupportedOperationException)
    }

    def 'identical versions should be equal'() {
        when:
        def version = new ServerVersion([3, 4, 1])

        then:
        version == new ServerVersion([3, 4, 1])
    }

    def 'identical versions should have the same hash code'() {
        when:
        def version = new ServerVersion([3, 4, 1])

        then:
        version.hashCode() == new ServerVersion([3, 4, 1]).hashCode()
    }

    def 'different versions should not be equal'() {
        when:
        def version = new ServerVersion([3, 4, 1])

        then:
        version != new ServerVersion([2, 5, 1])
    }

    def 'lower version should compare less than'() {
        when:
        def version = new ServerVersion([1, 5, 1])

        then:
        version.compareTo(new ServerVersion([2, 5, 1])) < 0

        when:
        version = new ServerVersion([2, 3, 1])

        then:
        version.compareTo(new ServerVersion([2, 5, 1])) < 0

        when:
        version = new ServerVersion([2, 5, 0])

        then:
        version.compareTo(new ServerVersion([2, 5, 1])) < 0
    }

    def 'higher version should compare greater than'() {
        when:
        def version = new ServerVersion([3, 6, 0])

        then:
        version.compareTo(new ServerVersion([3, 4, 1])) > 0

        when:
        version = new ServerVersion([3, 5, 1])

        then:
        version.compareTo(new ServerVersion([3, 4, 1])) > 0

        when:
        version = new ServerVersion([3, 4, 2])

        then:
        version.compareTo(new ServerVersion([3, 4, 1])) > 0
    }

    def 'same version should compare equal'() {
        when:
        def version = new ServerVersion([3, 4, 1])

        then:
        version.compareTo(new ServerVersion([3, 4, 1])) == 0
    }
}
