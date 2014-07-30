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





package com.mongodb.selector

import com.mongodb.ServerAddress
import com.mongodb.connection.ClusterDescription
import com.mongodb.connection.ServerDescription
import spock.lang.Specification

import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE
import static com.mongodb.connection.ClusterConnectionMode.SINGLE
import static com.mongodb.connection.ClusterType.REPLICA_SET
import static com.mongodb.connection.ClusterType.SHARDED
import static com.mongodb.connection.ServerConnectionState.CONNECTED
import static com.mongodb.connection.ServerConnectionState.CONNECTING
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY
import static com.mongodb.connection.ServerType.SHARD_ROUTER
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.MILLISECONDS

class MongosHAServerSelectorSpecification extends Specification {
    private MongosHAServerSelector selector;
    private ServerDescription first;
    private ServerDescription secondConnecting;
    private ServerDescription secondConnected;
    private ServerDescription replicaSetMember

    def setup() {
        selector = new MongosHAServerSelector();
        first = ServerDescription.builder()
                                 .state(CONNECTED)
                                 .address(new ServerAddress())
                                 .ok(true)
                                 .roundTripTime(10, MILLISECONDS)
                                 .type(SHARD_ROUTER)
                                 .build();

        secondConnecting = ServerDescription.builder()
                                            .state(CONNECTING)
                                            .address(new ServerAddress('localhost:27018'))
                                            .ok(false)
                                            .type(SHARD_ROUTER)
                                            .build();

        secondConnected = ServerDescription.builder()
                                           .state(CONNECTED)
                                           .address(new ServerAddress('localhost:27018'))
                                           .ok(true)
                                           .roundTripTime(8, MILLISECONDS)
                                           .type(SHARD_ROUTER)
                                           .build();
        replicaSetMember = ServerDescription.builder()
                                            .state(CONNECTED)
                                            .address(new ServerAddress('localhost:27018'))
                                            .ok(true)
                                            .roundTripTime(8, MILLISECONDS)
                                            .type(REPLICA_SET_PRIMARY)
                                            .build();

    }

    def 'should select any if connection mode is single'() throws UnknownHostException {
        expect:
        selector.select(new ClusterDescription(SINGLE, SHARDED, [secondConnected])) == [secondConnected]
    }

    def 'should select any if cluster type is not sharded'() throws UnknownHostException {
        expect:
        selector.select(new ClusterDescription(MULTIPLE, REPLICA_SET, [replicaSetMember])) == [replicaSetMember]
    }

    def 'should select fastest connected server'() {
        expect:
        selector.select(new ClusterDescription(MULTIPLE, SHARDED, [secondConnected, first])) == [secondConnected]
    }

    def shouldStickToNewServerIfStuckServerBecomesUnconnected() {
        given: // stick it
        selector.select(new ClusterDescription(MULTIPLE, SHARDED, asList(secondConnected, first)))

        expect:
        selector.select(new ClusterDescription(MULTIPLE, SHARDED, [secondConnecting, first])) == [first]
    }

    def shouldSwitchToFasterServerIfItWasNotPreviouslyConsidered() {
        given: // stick the first
        selector.select(new ClusterDescription(MULTIPLE, SHARDED, [secondConnecting, first]))

        expect:
        selector.select(new ClusterDescription(MULTIPLE, SHARDED, [secondConnected, first])) == [secondConnected]
    }
}