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

package com.mongodb.reactivestreams.client.internal


import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.async.FutureResultCallback
import com.mongodb.connection.ServerConnectionState
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerType
import com.mongodb.internal.binding.AsyncClusterAwareReadWriteBinding
import com.mongodb.internal.binding.AsyncClusterBinding
import com.mongodb.internal.binding.AsyncConnectionSource
import com.mongodb.internal.connection.Cluster
import com.mongodb.internal.connection.Server
import com.mongodb.internal.connection.ServerTuple
import com.mongodb.internal.session.ClientSessionContext
import com.mongodb.reactivestreams.client.ClientSession
import spock.lang.Specification

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT

class ClientSessionBindingSpecification extends Specification {

    def 'should return the session context from the connection source'() {
        given:
        def session = Stub(ClientSession)
        def wrappedBinding = Mock(AsyncClusterAwareReadWriteBinding);
        wrappedBinding.retain() >> wrappedBinding
        def binding = new ClientSessionBinding(session, false, wrappedBinding)

        when:
        def futureResultCallback = new FutureResultCallback<AsyncConnectionSource>()
        binding.getReadConnectionSource(OPERATION_CONTEXT, futureResultCallback)

        then:
        1 * wrappedBinding.getReadConnectionSource(OPERATION_CONTEXT, _) >> {
            it[1].onResult(Stub(AsyncConnectionSource), null)
        }

        when:
        futureResultCallback = new FutureResultCallback<AsyncConnectionSource>()
        binding.getWriteConnectionSource(OPERATION_CONTEXT, futureResultCallback)

        then:
        1 * wrappedBinding.getWriteConnectionSource(OPERATION_CONTEXT, _) >> {
            it[1].onResult(Stub(AsyncConnectionSource), null)
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
        def futureResultCallback = new FutureResultCallback<AsyncConnectionSource>()
        binding.getReadConnectionSource(OPERATION_CONTEXT, futureResultCallback)
        def readConnectionSource = futureResultCallback.get()
        futureResultCallback = new FutureResultCallback<AsyncConnectionSource>()
        binding.getWriteConnectionSource(OPERATION_CONTEXT, futureResultCallback)
        def writeConnectionSource = futureResultCallback.get()

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

    // TODO-JAVA-5640 move to SessionContext test
//    def 'owned session is implicit'() {
//        given:
//        def session = Mock(ClientSession)
//        def wrappedBinding = createStubBinding()
//
//        when:
//        def binding = new ClientSessionBinding(session, ownsSession, wrappedBinding)
//
//        then:
//        binding.getOperationContext(_).getSessionContext().isImplicitSession() == ownsSession
//
//        where:
//        ownsSession << [true, false]
//    }

    private AsyncClusterAwareReadWriteBinding createStubBinding() {
        def cluster = Mock(Cluster) {
            selectServerAsync(_, _, _) >> {
                it.last().onResult(new ServerTuple(Stub(Server), ServerDescription.builder()
                        .type(ServerType.STANDALONE)
                        .state(ServerConnectionState.CONNECTED)
                        .address(new ServerAddress())
                        .build()), null)
            }
        }
        new AsyncClusterBinding(cluster, ReadPreference.primary())
    }
}
