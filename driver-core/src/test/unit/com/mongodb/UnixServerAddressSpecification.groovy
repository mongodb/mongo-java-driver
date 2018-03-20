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

package com.mongodb

import spock.lang.Specification

class UnixServerAddressSpecification extends Specification {

    def 'should return the path for the host'() {
        when:
        def path = '/tmp/mongodb.sock'

        then:
        new UnixServerAddress(path).getHost() == path
    }

    def 'should throw if the path does not end with .sock'() {
        when:
        new UnixServerAddress('localhost').getSocketAddress()

        then:
        thrown(IllegalArgumentException)
    }

    def 'should throw when trying to get a InetSocketAddress'() {
        when:
        new UnixServerAddress('/tmp/mongodb.sock').getSocketAddress()

        then:
        thrown(UnsupportedOperationException)
    }
}
