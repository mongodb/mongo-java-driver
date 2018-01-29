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


class ServerIdSpecification extends Specification {
    def clusterId = new ClusterId()
    def serverAddress = new ServerAddress('host1')

    def 'should set all properties'() {
        given:
        def serverId = new ServerId(clusterId, serverAddress)

        expect:
        serverId.clusterId == clusterId
        serverId.address == serverAddress
    }

    def 'equivalent ids should be equal and have same hash code'() {
        def id1 = new ServerId(clusterId, serverAddress)
        def id2 = new ServerId(clusterId, serverAddress)

        expect:
        id1 == id2
        id1.hashCode() == id2.hashCode()
    }
}
