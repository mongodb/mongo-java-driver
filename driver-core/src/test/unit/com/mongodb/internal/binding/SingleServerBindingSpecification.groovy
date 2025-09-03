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

package com.mongodb.internal.binding

import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.connection.ServerConnectionState
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerType
import com.mongodb.internal.connection.Cluster
import com.mongodb.internal.connection.Server
import com.mongodb.internal.connection.ServerTuple
import spock.lang.Specification

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT

class SingleServerBindingSpecification extends Specification {

    def 'should implement getters'() {
        given:
        def cluster = Mock(Cluster) {
            selectServer(_, _) >> new ServerTuple(Mock(Server),
                    ServerDescription.builder()
                            .type(ServerType.STANDALONE)
                            .state(ServerConnectionState.CONNECTED)
                            .address(new ServerAddress())
                            .build())
        }
        def address = new ServerAddress()
        def operationContext = OPERATION_CONTEXT

        when:

        def binding = new SingleServerBinding(cluster, address, operationContext)

        then:
        binding.readPreference == ReadPreference.primary()
        binding.getOperationContext() == operationContext


        when:
        def source = binding.getReadConnectionSource()

        then:
        source.getOperationContext() == operationContext
    }

    def 'should increment and decrement reference counts'() {
        given:
        def cluster = Mock(Cluster) {
            selectServer(_, _) >> new ServerTuple(Mock(Server),
                    ServerDescription.builder()
                            .type(ServerType.STANDALONE)
                            .state(ServerConnectionState.CONNECTED)
                            .address(new ServerAddress())
                            .build())
        }
        def address = new ServerAddress()

        when:
        def binding = new SingleServerBinding(cluster, address, OPERATION_CONTEXT)

        then:
        binding.count == 1

        when:
        def source = binding.getReadConnectionSource()

        then:
        source.count == 1
        binding.count == 2

        when:
        source.retain()

        then:
        source.count == 2
        binding.count == 2

        when:
        source.release()

        then:
        source.count == 1
        binding.count == 2

        when:
        source.release()

        then:
        source.count == 0
        binding.count == 1

        when:
        source = binding.getWriteConnectionSource()

        then:
        source.count == 1
        binding.count == 2

        when:
        source.retain()

        then:
        source.count == 2
        binding.count == 2

        when:
        source.release()

        then:
        source.count == 1
        binding.count == 2

        when:
        source.release()

        then:
        source.count == 0
        binding.count == 1

        when:
        binding.release()

        then:
        binding.count == 0
    }
}
