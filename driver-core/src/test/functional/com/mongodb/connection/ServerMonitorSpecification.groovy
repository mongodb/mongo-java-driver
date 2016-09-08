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

package com.mongodb.connection

import com.mongodb.MongoException
import com.mongodb.MongoSocketException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ServerAddress
import com.mongodb.operation.CommandReadOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.codecs.BsonDocumentCodec

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static DefaultServerMonitor.exceptionHasChanged
import static DefaultServerMonitor.stateHasChanged
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.getCredentialList
import static com.mongodb.ClusterFixture.getPrimary
import static com.mongodb.ClusterFixture.getSslSettings

class ServerMonitorSpecification extends OperationFunctionalSpecification {
    ServerDescription newDescription
    ServerMonitor serverMonitor
    CountDownLatch latch = new CountDownLatch(1)

    def cleanup() {
        serverMonitor?.close();
    }

    def 'should have positive round trip time'() {
        given:
        initializeServerMonitor(getPrimary())

        when:
        latch.await()

        then:
        newDescription.roundTripTimeNanos > 0
    }

    def 'should return server version'() {
        given:
        initializeServerMonitor(getPrimary())

        def commandResult = new CommandReadOperation<BsonDocument>('admin', new BsonDocument('buildinfo', new BsonInt32(1)),
                                                                   new BsonDocumentCodec())
                .execute(getBinding())
        def expectedVersion = new ServerVersion(commandResult.getArray('versionArray')[0..2]*.value)
        when:
        latch.await()

        then:
        newDescription.version == expectedVersion

        cleanup:
        serverMonitor.close()
    }

    def 'should report current exception'() {
        given:
        initializeServerMonitor(new ServerAddress('some_unknown_server_name:34567'))

        when:
        latch.await()

        then:
        newDescription.exception instanceof MongoSocketException
    }

    def 'should report exception has changed when the current and previous are different'() {
        expect:
        exceptionHasChanged(null, new NullPointerException())
        exceptionHasChanged(new NullPointerException(), null)
        exceptionHasChanged(new SocketException(), new SocketException('A message'))
        exceptionHasChanged(new SocketException('A message'), new SocketException())
        exceptionHasChanged(new SocketException('A message'), new MongoException('A message'))
        exceptionHasChanged(new SocketException('A message'), new SocketException('A different message'))
    }

    def 'should report exception has not changed when the current and previous are the same'() {
        expect:
        !exceptionHasChanged(null, null)
        !exceptionHasChanged(new NullPointerException(), new NullPointerException())
        !exceptionHasChanged(new MongoException('A message'), new MongoException('A message'))
    }

    def 'should report state has changed if descriptions are different'() {
        expect:
        stateHasChanged(ServerDescription.builder()
                                         .type(ServerType.UNKNOWN)
                                         .state(ServerConnectionState.CONNECTING)
                                         .address(new ServerAddress())
                                         .build(),
                        ServerDescription.builder()
                                         .type(ServerType.STANDALONE)
                                         .state(ServerConnectionState.CONNECTED)
                                         .address(new ServerAddress())
                                         .roundTripTime(5, TimeUnit.MILLISECONDS)
                                         .build());
    }

    def 'should report state has changed if latencies are different'() {
        expect:
        stateHasChanged(ServerDescription.builder()
                                         .type(ServerType.STANDALONE)
                                         .state(ServerConnectionState.CONNECTED)
                                         .address(new ServerAddress())
                                         .roundTripTime(5, TimeUnit.MILLISECONDS)
                                         .build(),
                        ServerDescription.builder()
                                         .type(ServerType.STANDALONE)
                                         .state(ServerConnectionState.CONNECTED)
                                         .address(new ServerAddress())
                                         .roundTripTime(6, TimeUnit.MILLISECONDS)
                                         .build());
    }

    def 'should report state has not changed if descriptions and latencies are the same'() {
        expect:
        !stateHasChanged(ServerDescription.builder()
                                          .type(ServerType.STANDALONE)
                                          .state(ServerConnectionState.CONNECTED)
                                          .address(new ServerAddress())
                                          .roundTripTime(5, TimeUnit.MILLISECONDS)
                                          .lastUpdateTimeNanos(42L)
                                          .build(),
                         ServerDescription.builder()
                                          .type(ServerType.STANDALONE)
                                          .state(ServerConnectionState.CONNECTED)
                                          .address(new ServerAddress())
                                          .roundTripTime(5, TimeUnit.MILLISECONDS)
                                          .lastUpdateTimeNanos(42L)
                                          .build());
    }

    def initializeServerMonitor(ServerAddress address) {
        serverMonitor = new DefaultServerMonitor(new ServerId(new ClusterId(), address), ServerSettings.builder().build(),
                new ChangeListener<ServerDescription>() {
                    @Override
                    void stateChanged(final ChangeEvent<ServerDescription> event) {
                        newDescription = event.newValue
                        latch.countDown()
                    }
                },
                new InternalStreamConnectionFactory(new SocketStreamFactory(SocketSettings.builder().build(),
                        getSslSettings()),
                        getCredentialList(),
                        new NoOpConnectionListener(), null),
                new TestConnectionPool())
        serverMonitor.start()
        serverMonitor
    }
}
