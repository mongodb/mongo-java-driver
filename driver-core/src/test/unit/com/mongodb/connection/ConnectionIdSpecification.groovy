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

import com.mongodb.ServerAddress
import spock.lang.Specification

class ConnectionIdSpecification extends Specification {

    def serverId = new ServerId(new ClusterId(), new ServerAddress('host1'))

    def 'should set all properties'() {
        given:
        def id1 = new ConnectionId(serverId)
        def id2 = new ConnectionId(serverId, 11, 32)

        expect:
        id1.serverId == serverId
        id1.localValue > 0
        !id1.serverValue

        id2.serverId == serverId
        id2.localValue == 11
        id2.serverValue == 32
    }

    def 'should increment local value'() {
        given:
        def id1 = new ConnectionId(serverId)
        def id2 = new ConnectionId(serverId)

        expect:
        id2.localValue == id1.localValue + 1
    }


    def 'withServerValue should return a new instance with the given server value and preserve the rest'() {
        def id = new ConnectionId(serverId)

        expect:
        !id.withServerValue(124).is(id)
        id.withServerValue(123).serverValue == 123
        id.withServerValue(123).localValue == id.localValue
        id.withServerValue(123).serverId == serverId
    }

    def 'equivalent ids should be equal and have same hash code'() {
        given:
        def id1 = new ConnectionId(serverId, 100, 42)
        def id2 = new ConnectionId(serverId, 100, 42)

        expect:
        id2 == id1
        id2.hashCode() == id1.hashCode()
    }

    def 'different ids should be equal and have same hash code'() {
        given:
        def id1 = new ConnectionId(serverId, 100, 43)
        def id2 = new ConnectionId(serverId, 100, 42)

        expect:
        id2 != id1
        id2.hashCode() != id1.hashCode()
    }
}
