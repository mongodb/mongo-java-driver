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

package com.mongodb.internal.selector

import com.mongodb.ServerAddress
import com.mongodb.connection.ClusterDescription
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerType
import spock.lang.Specification

import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE
import static com.mongodb.connection.ClusterConnectionMode.SINGLE
import static com.mongodb.connection.ClusterType.REPLICA_SET
import static com.mongodb.connection.ServerConnectionState.CONNECTED

class ReadPreferenceServerSelectorSpecification extends Specification {

    def primary = ServerDescription.builder()
                                   .state(CONNECTED)
                                   .address(new ServerAddress())
                                   .ok(true)
                                   .type(ServerType.REPLICA_SET_PRIMARY)
                                   .build()
    def secondary = ServerDescription.builder()
                                     .state(CONNECTED)
                                     .address(new ServerAddress('localhost', 27018))
                                     .ok(true)
                                     .type(ServerType.REPLICA_SET_SECONDARY)
                                     .build()

    def 'constructor should throws if read preference is null'() {
        when:
        new ReadPreferenceServerSelector(null)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should get read preference'() {
        expect:
        new ReadPreferenceServerSelector(primary()).readPreference == primary()
    }

    def 'should override toString'() {
        expect:
        new ReadPreferenceServerSelector(primary()).toString() == 'ReadPreferenceServerSelector{readPreference=primary}'
    }

    def 'should select server that matches read preference when connection mode is multiple'() {
        expect:
        new ReadPreferenceServerSelector(primary()).select(new ClusterDescription(MULTIPLE, REPLICA_SET, [primary, secondary])) ==
        [primary]
        new ReadPreferenceServerSelector(secondary()).select(new ClusterDescription(MULTIPLE, REPLICA_SET, [primary, secondary])) ==
        [secondary]
    }

    def 'should select any ok server when connection mode is single'() {
        expect:
        new ReadPreferenceServerSelector(primary()).select(new ClusterDescription(SINGLE, REPLICA_SET, [secondary])) == [secondary]
    }
}
