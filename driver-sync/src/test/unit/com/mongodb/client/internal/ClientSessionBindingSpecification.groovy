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

package com.mongodb.client.internal

import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.client.ClientSession
import com.mongodb.connection.ClusterConnectionMode
import com.mongodb.connection.ClusterDescription
import com.mongodb.connection.ClusterType
import com.mongodb.internal.IgnorableRequestContext
import com.mongodb.internal.binding.ClusterBinding
import com.mongodb.internal.binding.ConnectionSource
import com.mongodb.internal.binding.ReadWriteBinding
import com.mongodb.internal.connection.Cluster
import com.mongodb.internal.session.ClientSessionContext
import spock.lang.Specification

class ClientSessionBindingSpecification extends Specification {
    def 'should return the session context from the binding'() {
        given:
        def session = Stub(ClientSession)
        def wrappedBinding = Stub(ClusterBinding)
        def binding = new ClientSessionBinding(session, false, wrappedBinding)

        when:
        def context = binding.getSessionContext()

        then:
        (context as ClientSessionContext).getClientSession() == session
    }

    def 'should return the session context from the connection source'() {
        given:
        def session = Stub(ClientSession)
        def wrappedBinding = Mock(ClusterBinding) {
            getCluster() >> {
                Mock(Cluster) {
                    getDescription() >> {
                        new ClusterDescription(ClusterConnectionMode.SINGLE, ClusterType.STANDALONE, [])
                    }
                }
            }
        }
        def binding = new ClientSessionBinding(session, false, wrappedBinding)

        when:
        def readConnectionSource = binding.getReadConnectionSource()
        def context = readConnectionSource.getSessionContext()

        then:
        (context as ClientSessionContext).getClientSession() == session
        1 * wrappedBinding.getReadConnectionSource() >> {
            Stub(ConnectionSource)
        }

        when:
        def writeConnectionSource = binding.getWriteConnectionSource()
        context = writeConnectionSource.getSessionContext()

        then:
        (context as ClientSessionContext).getClientSession() == session
        1 * wrappedBinding.getWriteConnectionSource() >> {
            Stub(ConnectionSource)
        }
    }

    def 'should close client session when binding reference count drops to zero if it is owned by the binding'() {
        given:
        def session = Mock(ClientSession)
        def wrappedBinding = createStubBinding()
        def binding = new ClientSessionBinding(session, true, wrappedBinding)
        binding.retain()

        when:
        binding.release()

        then:
        0 * session.close()

        when:
        binding.release()

        then:
        1 * session.close()
    }

    def 'should close client session when binding reference count drops to zero due to connection source if it is owned by the binding'() {
        given:
        def session = Mock(ClientSession)
        def wrappedBinding = createStubBinding()
        def binding = new ClientSessionBinding(session, true, wrappedBinding)
        def readConnectionSource = binding.getReadConnectionSource()
        def writeConnectionSource = binding.getWriteConnectionSource()

        when:
        binding.release()

        then:
        0 * session.close()

        when:
        writeConnectionSource.release()

        then:
        0 * session.close()

        when:
        readConnectionSource.release()

        then:
        1 * session.close()
    }

    def 'should not close client session when binding reference count drops to zero if it is not owned by the binding'() {
        given:
        def session = Mock(ClientSession)
        def wrappedBinding = createStubBinding()
        def binding = new ClientSessionBinding(session, false, wrappedBinding)
        binding.retain()

        when:
        binding.release()

        then:
        0 * session.close()

        when:
        binding.release()

        then:
        0 * session.close()
    }

    def 'owned session is implicit'() {
        given:
        def session = Mock(ClientSession)
        def wrappedBinding = createStubBinding()

        when:
        def binding = new ClientSessionBinding(session, ownsSession, wrappedBinding)

        then:
        binding.getSessionContext().isImplicitSession() == ownsSession

        where:
        ownsSession << [true, false]
    }

    private ReadWriteBinding createStubBinding() {
        def cluster = Stub(Cluster)
        new ClusterBinding(cluster, ReadPreference.primary(), ReadConcern.DEFAULT, null, IgnorableRequestContext.INSTANCE)
    }
}
