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
package com.mongodb.internal.connection;

import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.ServerType;
import com.mongodb.event.ServerHeartbeatFailedEvent;
import com.mongodb.event.ServerHeartbeatStartedEvent;
import com.mongodb.event.ServerHeartbeatSucceededEvent;
import com.mongodb.event.ServerMonitorListener;
import com.mongodb.event.TestServerMonitorListener;
import com.mongodb.internal.inject.SameObjectProvider;
import org.bson.BsonDocument;
import org.bson.ByteBufNIO;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT_FACTORY;
import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.internal.connection.MessageHelper.LEGACY_HELLO_LOWER;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class DefaultServerMonitorTest {

    private DefaultServerMonitor monitor;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (monitor != null) {
            monitor.close();
            monitor.getServerMonitor().join();
        }
    }

    @Test
    void closeShouldNotSendStateChangedEvent() throws Exception {
        // Given
        AtomicBoolean stateChanged = new AtomicBoolean(false);

        SdamServerDescriptionManager sdamManager = new SdamServerDescriptionManager() {
            @Override
            public void update(final ServerDescription candidateDescription) {
                assertNotNull(candidateDescription);
                stateChanged.set(true);
            }

            @Override
            public void handleExceptionBeforeHandshake(final SdamServerDescriptionManager.SdamIssue sdamIssue) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void handleExceptionAfterHandshake(final SdamServerDescriptionManager.SdamIssue sdamIssue) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SdamServerDescriptionManager.SdamIssue.Context context() {
                throw new UnsupportedOperationException();
            }

            @Override
            public SdamServerDescriptionManager.SdamIssue.Context context(final InternalConnection connection) {
                throw new UnsupportedOperationException();
            }
        };

        InternalConnection mockConnection = mock(InternalConnection.class);
        doAnswer(invocation -> {
            Thread.sleep(100);
            return null;
        }).when(mockConnection).open(any());

        InternalConnectionFactory factory = createConnectionFactory(mockConnection);

        monitor = new DefaultServerMonitor(
                new ServerId(new ClusterId(), new ServerAddress()),
                ServerSettings.builder().build(),
                factory,
                ClusterConnectionMode.SINGLE,
                null,
                false,
                SameObjectProvider.initialized(sdamManager),
                OPERATION_CONTEXT_FACTORY);

        // When
        monitor.start();
        monitor.close();

        // Then
        assertFalse(stateChanged.get());
    }

    @Test
    void shouldSendStartedAndSucceededHeartbeatEvents() throws Exception {
        // Given
        ConnectionDescription connectionDescription = createDefaultConnectionDescription();
        ServerDescription initialServerDescription = createDefaultServerDescription();

        String helloResponse = "{"
                + LEGACY_HELLO_LOWER + ": true,"
                + "maxBsonObjectSize : 16777216, "
                + "maxMessageSizeBytes : 48000000, "
                + "maxWriteBatchSize : 1000, "
                + "localTime : ISODate(\"2016-04-05T20:36:36.082Z\"), "
                + "maxWireVersion : 4, "
                + "minWireVersion : 0, "
                + "ok : 1 "
                + "}";

        InternalConnection mockConnection = mock(InternalConnection.class);
        when(mockConnection.getDescription()).thenReturn(connectionDescription);
        when(mockConnection.getInitialServerDescription()).thenReturn(initialServerDescription);
        when(mockConnection.getBuffer(anyInt())).thenReturn(new ByteBufNIO(ByteBuffer.allocate(1024)));
        when(mockConnection.receive(any(), any())).thenReturn(BsonDocument.parse(helloResponse));

        // When
        TestServerMonitorListener listener = createTestServerMonitorListener();
        monitor = createAndStartMonitor(createConnectionFactory(mockConnection), listener);

        listener.waitForEvents(ServerHeartbeatSucceededEvent.class, event -> true, 1, Duration.ofSeconds(30));
        ServerHeartbeatStartedEvent startedEvent = getEvent(ServerHeartbeatStartedEvent.class, listener);
        ServerHeartbeatSucceededEvent succeededEvent = getEvent(ServerHeartbeatSucceededEvent.class, listener);

        // Then
        assertEquals(connectionDescription.getConnectionId(), startedEvent.getConnectionId());
        assertEquals(connectionDescription.getConnectionId(), succeededEvent.getConnectionId());
        assertEquals(BsonDocument.parse(helloResponse), succeededEvent.getReply());
        assertTrue(succeededEvent.getElapsedTime(TimeUnit.NANOSECONDS) > 0);
    }

    @Test
    void shouldSendStartedAndFailedHeartbeatEvents() throws Exception {
        // Given
        ConnectionDescription connectionDescription = createDefaultConnectionDescription();
        ServerDescription initialServerDescription = createDefaultServerDescription();
        MongoSocketReadTimeoutException exception = new MongoSocketReadTimeoutException("read timeout",
                new ServerAddress(), new IOException());

        InternalConnection mockConnection = mock(InternalConnection.class);
        when(mockConnection.getDescription()).thenReturn(connectionDescription);
        when(mockConnection.getInitialServerDescription()).thenReturn(initialServerDescription);
        when(mockConnection.getBuffer(anyInt())).thenReturn(new ByteBufNIO(ByteBuffer.allocate(1024)));
        when(mockConnection.receive(any(), any())).thenThrow(exception);

        // When
        TestServerMonitorListener listener = createTestServerMonitorListener();
        monitor = createAndStartMonitor(createConnectionFactory(mockConnection), listener);

        listener.waitForEvents(ServerHeartbeatFailedEvent.class, event -> true, 1, Duration.ofSeconds(30));
        ServerHeartbeatStartedEvent startedEvent = getEvent(ServerHeartbeatStartedEvent.class, listener);
        ServerHeartbeatFailedEvent failedEvent = getEvent(ServerHeartbeatFailedEvent.class, listener);

        // Then
        assertEquals(connectionDescription.getConnectionId(), startedEvent.getConnectionId());
        assertEquals(connectionDescription.getConnectionId(), failedEvent.getConnectionId());
        assertEquals(exception, failedEvent.getThrowable());
        assertTrue(failedEvent.getElapsedTime(TimeUnit.NANOSECONDS) > 0);
    }

    @Test
    void shouldEmitHeartbeatStartedBeforeSocketIsConnected() throws Exception {
        // Given
        InternalConnection mockConnection = mock(InternalConnection.class);

        AtomicBoolean firstHeartBeat = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);
        List<String> events = new ArrayList<>();
        ServerMonitorListener listener = new ServerMonitorListener() {
                @Override
                public void serverHearbeatStarted(final ServerHeartbeatStartedEvent event) {
                    events.add("serverHeartbeatStartedEvent");
                }

                @Override
                public void serverHeartbeatSucceeded(final ServerHeartbeatSucceededEvent event) {
                    events.add("serverHeartbeatSucceededEvent");
                    latch.countDown();
                }

                @Override
                public void serverHeartbeatFailed(final ServerHeartbeatFailedEvent event) {
                    events.add("serverHeartbeatFailedEvent");
                    latch.countDown();
                }
            };

        doAnswer(invocation -> {
            events.add("client connected");
            return null;
        }).when(mockConnection).open(any());

        when(mockConnection.getBuffer(anyInt())).thenReturn(new ByteBufNIO(ByteBuffer.allocate(1024)));
        when(mockConnection.getDescription()).thenReturn(createDefaultConnectionDescription());
        when(mockConnection.getInitialServerDescription()).thenReturn(createDefaultServerDescription());

        doAnswer(invocation -> {
            events.add("client hello received");
            throw new SocketException("Socket error");
        }).when(mockConnection).receive(any(), any());

        // When
        monitor = createAndStartMonitor(createConnectionFactory(mockConnection), listener);
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Timed out waiting for heartbeat");

        // Then
        List<String> expectedEvents = asList("serverHeartbeatStartedEvent", "client connected", "client hello received", "serverHeartbeatFailedEvent");
        assertEquals(expectedEvents, events);
    }


    private InternalConnectionFactory createConnectionFactory(final InternalConnection connection) {
        InternalConnectionFactory factory = mock(InternalConnectionFactory.class);
        when(factory.create(any())).thenReturn(connection);
        return factory;
    }

    private ServerDescription createDefaultServerDescription() {
        return ServerDescription.builder()
                .ok(true)
                .address(new ServerAddress())
                .type(ServerType.STANDALONE)
                .state(ServerConnectionState.CONNECTED)
                .build();
    }

    private ConnectionDescription createDefaultConnectionDescription() {
        return new ConnectionDescription(new ServerId(new ClusterId(""), new ServerAddress()));
    }

    private DefaultServerMonitor createAndStartMonitor(final InternalConnectionFactory factory, final ServerMonitorListener listener) {
        DefaultServerMonitor monitor = new DefaultServerMonitor(
                new ServerId(new ClusterId(), new ServerAddress()),
                ServerSettings.builder()
                        .heartbeatFrequency(500, TimeUnit.MILLISECONDS)
                        .addServerMonitorListener(listener)
                        .build(),
                factory,
                ClusterConnectionMode.SINGLE,
                null,
                false,
                SameObjectProvider.initialized(mock(SdamServerDescriptionManager.class)),
                OPERATION_CONTEXT_FACTORY);
        monitor.start();
        return monitor;
    }

    private <T> T getEvent(final Class<T> clazz, final TestServerMonitorListener listener) {
        return listener.getEvents()
                .stream()
                .filter(clazz::isInstance)
                .map(clazz::cast)
                .findFirst()
                .orElseThrow(AssertionFailedError::new);
    }

    private TestServerMonitorListener createTestServerMonitorListener() {
        return new TestServerMonitorListener(asList("serverHeartbeatStartedEvent", "serverHeartbeatSucceededEvent",
                "serverHeartbeatFailedEvent"));
    }
}
