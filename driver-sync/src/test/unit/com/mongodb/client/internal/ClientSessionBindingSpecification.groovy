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


import com.mongodb.ReadPreference
import com.mongodb.client.ClientSession
import com.mongodb.internal.binding.ClusterBinding
import com.mongodb.internal.binding.ConnectionSource
import com.mongodb.internal.binding.ReadWriteBinding
import com.mongodb.internal.connection.Cluster
import spock.lang.Specification

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT

class ClientSessionBindingSpecification extends Specification {

    def 'should call underlying wrapped binding'() {
        given:
        def session = Stub(ClientSession)
        def wrappedBinding = Mock(ClusterBinding);
        def binding = new ClientSessionBinding(session, false, wrappedBinding)

        when:
        binding.getReadConnectionSource(OPERATION_CONTEXT)

        then:
        1 * wrappedBinding.getReadConnectionSource(OPERATION_CONTEXT) >> {
            Stub(ConnectionSource)
        }

        when:
        binding.getWriteConnectionSource(OPERATION_CONTEXT)

        then:
        1 * wrappedBinding.getWriteConnectionSource(OPERATION_CONTEXT) >> {
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
        def readConnectionSource = binding.getReadConnectionSource(OPERATION_CONTEXT)
        def writeConnectionSource = binding.getWriteConnectionSource(OPERATION_CONTEXT)

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

    //TODO move to SessionContext test
//    def 'owned session is implicit'() {
//        given:
//        def session = Mock(ClientSession)
//        def wrappedBinding = createStubBinding()
//
//        when:
//        def binding = new ClientSessionBinding(session, ownsSession, wrappedBinding)
//
//        then:
//        binding.getOperationContext().getSessionContext().isImplicitSession() == ownsSession
//
//        where:
//        ownsSession << [true, false]
//    }

    private ReadWriteBinding createStubBinding() {
        def cluster = Stub(Cluster)
        new ClusterBinding(cluster, ReadPreference.primary())
    }
}
