/*
 * Copyright 2008-2016 MongoDB, Inc.
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

import com.mongodb.MongoSocketReadTimeoutException
import com.mongodb.ServerAddress
import com.mongodb.event.ServerHeartbeatFailedEvent
import com.mongodb.event.ServerHeartbeatStartedEvent
import com.mongodb.event.ServerHeartbeatSucceededEvent
import com.mongodb.event.ServerMonitorListener
import org.bson.BsonDocument
import org.bson.ByteBufNIO
import spock.lang.Specification

import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@SuppressWarnings('BusyWait')
class DefaultServerMonitorSpecification extends Specification {

    DefaultServerMonitor monitor

    def 'close should not send a sendStateChangedEvent'() {
        given:
        def stateChanged = false
        def changeListener = new ChangeListener<ServerDescription>() {
            @Override
            void stateChanged(final ChangeEvent<ServerDescription> event) {
                stateChanged = true
            }
        }
        def internalConnectionFactory = Mock(InternalConnectionFactory) {
            create(_) >> {
                Mock(InternalConnection) {
                    open() >> { sleep(100) }
                }
            }
        }
        monitor = new DefaultServerMonitor(new ServerId(new ClusterId(), new ServerAddress()), ServerSettings.builder().build(),
                changeListener, internalConnectionFactory, new TestConnectionPool())
        monitor.start()

        when:
        monitor.close()
        monitor.monitorThread.join()

        then:
        !stateChanged
    }

    def 'should send started and succeeded heartbeat events'() {
        given:
        def changeListener = new ChangeListener<ServerDescription>() {
            @Override
            void stateChanged(final ChangeEvent<ServerDescription> event) {
            }
        }

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

        def isMasterResponse = '{' +
                'ismaster : true, ' +
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
                    open() >> { }

                    getBuffer(_) >> { int size ->
                        new ByteBufNIO(ByteBuffer.allocate(size))
                    }

                    getDescription() >> {
                       connectionDescription
                    }

                    sendMessage(_, _) >> { }

                    sendAndReceive(_, _) >> {
                        BsonDocument.parse(isMasterResponse)
                    }
                }
            }
        }
        monitor = new DefaultServerMonitor(new ServerId(new ClusterId(), new ServerAddress()),
                ServerSettings.builder().heartbeatFrequency(1, TimeUnit.HOURS).addServerMonitorListener(serverMonitorListener).build(),
                changeListener, internalConnectionFactory, new TestConnectionPool())

        when:
        monitor.start()
        latch.await(30, TimeUnit.SECONDS)

        then:
        failedEvent == null
        startedEvent.connectionId == connectionDescription.connectionId
        succeededEvent.connectionId == connectionDescription.connectionId
        succeededEvent.reply == BsonDocument.parse(isMasterResponse)
        succeededEvent.getElapsedTime(TimeUnit.NANOSECONDS) > 0

        cleanup:
        monitor?.close()
    }

    def 'should send started and failed heartbeat events'() {
        given:
        def changeListener = new ChangeListener<ServerDescription>() {
            @Override
            void stateChanged(final ChangeEvent<ServerDescription> event) {
            }
        }

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
        def exception = new MongoSocketReadTimeoutException('read timeout', new ServerAddress(), new IOException())

        def internalConnectionFactory = Mock(InternalConnectionFactory) {
            create(_) >> {
                Mock(InternalConnection) {
                    open() >> { }

                    getBuffer(_) >> { int size ->
                        new ByteBufNIO(ByteBuffer.allocate(size))
                    }

                    getDescription() >> {
                        connectionDescription
                    }

                    sendAndReceive(_, _) >> {
                        throw exception
                    }
                }
            }
        }
        monitor = new DefaultServerMonitor(new ServerId(new ClusterId(), new ServerAddress()),
                ServerSettings.builder().heartbeatFrequency(1, TimeUnit.HOURS).addServerMonitorListener(serverMonitorListener).build(),
                changeListener, internalConnectionFactory, new TestConnectionPool())

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
}
