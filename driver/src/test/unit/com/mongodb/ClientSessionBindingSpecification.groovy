/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb

import com.mongodb.binding.ClusterBinding
import com.mongodb.binding.ConnectionSource
import com.mongodb.binding.ReadWriteBinding
import com.mongodb.connection.Cluster
import spock.lang.Specification

class ClientSessionBindingSpecification extends Specification {
    def 'should return the session context from the binding'() {
        given:
        def session = Stub(ClientSession)
        def wrappedBinding = Stub(ReadWriteBinding)
        def binding = new ClientSessionBinding(session, false, wrappedBinding)

        when:
        def context = binding.getSessionContext()

        then:
        (context as ClientSessionContext).getClientSession() == session
    }

    def 'should return the session context from the connection source'() {
        given:
        def session = Stub(ClientSession)
        def wrappedBinding = Mock(ReadWriteBinding)
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

    private ReadWriteBinding createStubBinding() {
        def cluster = Stub(Cluster)
        new ClusterBinding(cluster, ReadPreference.primary())
    }
}
