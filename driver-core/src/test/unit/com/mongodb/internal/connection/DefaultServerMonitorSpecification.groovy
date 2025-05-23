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

package com.mongodb.internal.connection

import com.mongodb.MongoSocketReadTimeoutException
import com.mongodb.ServerAddress
import com.mongodb.connection.ClusterConnectionMode
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerConnectionState
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerId
import com.mongodb.connection.ServerSettings
import com.mongodb.connection.ServerType
import com.mongodb.event.ServerHeartbeatFailedEvent
import com.mongodb.event.ServerHeartbeatStartedEvent
import com.mongodb.event.ServerHeartbeatSucceededEvent
import com.mongodb.event.ServerMonitorListener
import com.mongodb.internal.inject.SameObjectProvider
import org.bson.BsonDocument
import org.bson.ByteBufNIO
import spock.lang.Specification

import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT_FACTORY
import static com.mongodb.internal.connection.MessageHelper.LEGACY_HELLO_LOWER

@SuppressWarnings('BusyWait')
class DefaultServerMonitorSpecification extends Specification {

    DefaultServerMonitor monitor

    def 'close should not send a sendStateChangedEvent'() {
        given:
        def stateChanged = false
        def sdam = new SdamServerDescriptionManager() {
            @Override
            void monitorUpdate(final ServerDescription candidateDescription) {
                assert candidateDescription != null
                stateChanged = true
            }

            @Override
            void update(final ServerDescription candidateDescription) {
                assert candidateDescription != null
                stateChanged = true
            }

            @Override
            void handleExceptionBeforeHandshake(final SdamServerDescriptionManager.SdamIssue sdamIssue) {
                throw new UnsupportedOperationException()
            }

            @Override
            void handleExceptionAfterHandshake(final SdamServerDescriptionManager.SdamIssue sdamIssue) {
                throw new UnsupportedOperationException()
            }

            @Override
            SdamServerDescriptionManager.SdamIssue.Context context() {
                throw new UnsupportedOperationException()
            }

            @Override
            SdamServerDescriptionManager.SdamIssue.Context context(final InternalConnection connection) {
                throw new UnsupportedOperationException()
            }
        }
        def internalConnectionFactory = Mock(InternalConnectionFactory) {
            create(_) >> {
                Mock(InternalConnection) {
                    open(_) >> { sleep(100) }
                }
            }
        }
        monitor = new DefaultServerMonitor(new ServerId(new ClusterId(), new ServerAddress()), ServerSettings.builder().build(),
                internalConnectionFactory, ClusterConnectionMode.SINGLE, null, false, SameObjectProvider.initialized(sdam),
                OPERATION_CONTEXT_FACTORY)

        monitor.start()

        when:
        monitor.close()
        monitor.monitor.join()

        then:
        !stateChanged
    }

    def 'should send started and succeeded heartbeat events'() {
        given:
        def latch = new CountDownLatch(1)
        def startedEvent
        def succeededEvent
        def failedEvent

        def serverMonitorListener = new ServerMonitorListener() {
            @Override
            void serverHearbeatStarted(final ServerHeartbeatStartedEvent event) {
                startedEvent = event
            }

            @Override
            void serverHeartbeatSucceeded(final ServerHeartbeatSucceededEvent event) {
                succeededEvent = event
                latch.countDown()
            }

            @Override
            void serverHeartbeatFailed(final ServerHeartbeatFailedEvent event) {
                failedEvent = event
                latch.countDown()
            }
        }

        def connectionDescription = new ConnectionDescription(new ServerId(new ClusterId(''), new ServerAddress()))
        def initialServerDescription = ServerDescription.builder()
                .ok(true)
                .address(new ServerAddress())
                .type(ServerType.STANDALONE)
                .state(ServerConnectionState.CONNECTED)
                .build()

        def helloResponse = '{' +
                "$LEGACY_HELLO_LOWER: true," +
                'maxBsonObjectSize : 16777216, ' +
                'maxMessageSizeBytes : 48000000, ' +
                'maxWriteBatchSize : 1000, ' +
                'localTime : ISODate("2016-04-05T20:36:36.082Z"), ' +
                'maxWireVersion : 4, ' +
                'minWireVersion : 0, ' +
                'ok : 1 ' +
                '}'

        def internalConnectionFactory = Mock(InternalConnectionFactory) {
            create(_) >> {
                Mock(InternalConnection) {
                    open(_) >> { }

                    getBuffer(_) >> { int size ->
                        new ByteBufNIO(ByteBuffer.allocate(size))
                    }

                    getDescription() >> {
                        connectionDescription
                    }

                    getInitialServerDescription() >> {
                        initialServerDescription
                    }

                    send(_, _, _) >> { }

                    receive(_, _) >> {
                        BsonDocument.parse(helloResponse)
                    }
                }
            }
        }
        monitor = new DefaultServerMonitor(new ServerId(new ClusterId(), new ServerAddress()),
                ServerSettings.builder().heartbeatFrequency(1, TimeUnit.SECONDS).addServerMonitorListener(serverMonitorListener).build(),
                internalConnectionFactory, ClusterConnectionMode.SINGLE, null, false, mockSdamProvider(), OPERATION_CONTEXT_FACTORY)

        when:
        monitor.start()
        latch.await(30, TimeUnit.SECONDS)

        then:
        failedEvent == null
        startedEvent.connectionId == connectionDescription.connectionId
        succeededEvent.connectionId == connectionDescription.connectionId
        succeededEvent.reply == BsonDocument.parse(helloResponse)
        succeededEvent.getElapsedTime(TimeUnit.NANOSECONDS) > 0

        cleanup:
        monitor?.close()
    }

    def 'should send started and failed heartbeat events'() {
        given:
        def latch = new CountDownLatch(1)
        def startedEvent
        def succeededEvent
        def failedEvent

        def serverMonitorListener = new ServerMonitorListener() {
            @Override
            void serverHearbeatStarted(final ServerHeartbeatStartedEvent event) {
                startedEvent = event
            }

            @Override
            void serverHeartbeatSucceeded(final ServerHeartbeatSucceededEvent event) {
                succeededEvent = event
                latch.countDown()
            }

            @Override
            void serverHeartbeatFailed(final ServerHeartbeatFailedEvent event) {
                failedEvent = event
                latch.countDown()
            }
        }

        def connectionDescription = new ConnectionDescription(new ServerId(new ClusterId(''), new ServerAddress()))
        def initialServerDescription = ServerDescription.builder()
                .ok(true)
                .address(new ServerAddress())
                .type(ServerType.STANDALONE)
                .state(ServerConnectionState.CONNECTED)
                .build()
        def exception = new MongoSocketReadTimeoutException('read timeout', new ServerAddress(), new IOException())

        def internalConnectionFactory = Mock(InternalConnectionFactory) {
            create(_) >> {
                Mock(InternalConnection) {
                    open(_) >> { }

                    getBuffer(_) >> { int size ->
                        new ByteBufNIO(ByteBuffer.allocate(size))
                    }

                    getDescription() >> {
                        connectionDescription
                    }

                    getInitialServerDescription() >> {
                        initialServerDescription
                    }

                    send(_, _, _) >> { }

                    receive(_, _) >> {
                        throw exception
                    }
                }
            }
        }
        monitor = new DefaultServerMonitor(new ServerId(new ClusterId(), new ServerAddress()),
                ServerSettings.builder().heartbeatFrequency(1, TimeUnit.SECONDS).addServerMonitorListener(serverMonitorListener).build(),
                internalConnectionFactory, ClusterConnectionMode.SINGLE, null, false, mockSdamProvider(), OPERATION_CONTEXT_FACTORY)

        when:
        monitor.start()
        latch.await(30, TimeUnit.SECONDS)

        then:
        succeededEvent == null
        startedEvent.connectionId == connectionDescription.connectionId
        failedEvent.connectionId == connectionDescription.connectionId
        failedEvent.throwable == exception
        failedEvent.getElapsedTime(TimeUnit.NANOSECONDS) > 0

        cleanup:
        monitor?.close()
    }

    private mockSdamProvider() {
        SameObjectProvider.initialized(Mock(SdamServerDescriptionManager))
    }
}
