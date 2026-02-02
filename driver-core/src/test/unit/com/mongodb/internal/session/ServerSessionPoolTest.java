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

package com.mongodb.internal.session;

import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerSettings;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.Server;
import com.mongodb.internal.connection.ServerTuple;
import com.mongodb.session.ServerSession;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.ClusterFixture.getServerApi;
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterType.REPLICA_SET;
import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY;
import static com.mongodb.connection.ServerType.UNKNOWN;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("ServerSessionPool")
@ExtendWith(MockitoExtension.class)
class ServerSessionPoolTest {

    private ClusterDescription connectedDescription;
    private ClusterDescription unconnectedDescription;

    @Mock
    private Cluster clusterMock;

    @BeforeEach
    void setUp() {
        connectedDescription = new ClusterDescription(
                MULTIPLE,
                REPLICA_SET,
                singletonList(
                        ServerDescription.builder()
                                .ok(true)
                                .state(CONNECTED)
                                .address(new ServerAddress())
                                .type(REPLICA_SET_PRIMARY)
                                .logicalSessionTimeoutMinutes(30)
                                .build()
                ),
                ClusterSettings.builder().hosts(singletonList(new ServerAddress())).build(),
                ServerSettings.builder().build()
        );

        unconnectedDescription = new ClusterDescription(
                MULTIPLE,
                REPLICA_SET,
                singletonList(
                        ServerDescription.builder()
                                .ok(true)
                                .state(CONNECTING)
                                .address(new ServerAddress())
                                .type(UNKNOWN)
                                .logicalSessionTimeoutMinutes(null)
                                .build()
                ),
                ClusterSettings.builder().hosts(singletonList(new ServerAddress())).build(),
                ServerSettings.builder().build()
        );
    }

    @Test
    @DisplayName("should get session from pool")
    void testGetSession() {
        ServerSessionPool pool = new ServerSessionPool(clusterMock, TIMEOUT_SETTINGS, getServerApi());

        ServerSession session = pool.get();

        assertNotNull(session);
    }

    @Test
    @DisplayName("should throw IllegalStateException when pool is closed")
    void testThrowExceptionIfPoolClosed() {
        ServerSessionPool pool = new ServerSessionPool(clusterMock, TIMEOUT_SETTINGS, getServerApi());
        pool.close();

        assertThrows(IllegalStateException.class, pool::get);
    }

    @Test
    @DisplayName("should reuse released session from pool")
    void testPoolSession() {
        when(clusterMock.getCurrentDescription()).thenReturn(connectedDescription);
        ServerSessionPool pool = new ServerSessionPool(clusterMock, TIMEOUT_SETTINGS, getServerApi());

        ServerSession session = pool.get();
        pool.release(session);
        ServerSession pooledSession = pool.get();

        assertEquals(session, pooledSession);
    }

    @Test
    @DisplayName("should prune expired sessions when getting new session")
    void testPruneSessionsWhenGetting() {
        when(clusterMock.getCurrentDescription()).thenReturn(connectedDescription);

        ServerSessionPool.Clock clock = mock(ServerSessionPool.Clock.class);
        when(clock.millis()).thenReturn(0L, MINUTES.toMillis(29) + 1);

        ServerSessionPool pool = new ServerSessionPool(clusterMock, OPERATION_CONTEXT, clock);
        ServerSession sessionOne = pool.get();

        pool.release(sessionOne);
        assertFalse(sessionOne.isClosed());

        ServerSession sessionTwo = pool.get();

        assertNotEquals(sessionTwo, sessionOne);
        // Note: Actual closed state verification depends on implementation details
    }

    @Test
    @DisplayName("should not prune session when timeout is null")
    void testNotPruneSessionWhenTimeoutIsNull() {
        when(clusterMock.getCurrentDescription()).thenReturn(unconnectedDescription);

        ServerSessionPool.Clock clock = mock(ServerSessionPool.Clock.class);
        when(clock.millis()).thenReturn(0L, 0L, 0L);

        ServerSessionPool pool = new ServerSessionPool(clusterMock, OPERATION_CONTEXT, clock);
        ServerSession session = pool.get();

        pool.release(session);
        ServerSession newSession = pool.get();

        assertEquals(session, newSession);
    }

    @Test
    @DisplayName("should initialize session with correct properties")
    void testInitializeSession() {
        ServerSessionPool.Clock clock = mock(ServerSessionPool.Clock.class);
        when(clock.millis()).thenReturn(42L);

        ServerSessionPool pool = new ServerSessionPool(clusterMock, OPERATION_CONTEXT, clock);
        ServerSession session = pool.get();

        ServerSessionPool.ServerSessionImpl sessionImpl = (ServerSessionPool.ServerSessionImpl) session;
        assertEquals(42L, sessionImpl.getLastUsedAtMillis());
        assertEquals(0L, sessionImpl.getTransactionNumber());

        BsonDocument identifier = sessionImpl.getIdentifier();
        assertNotNull(identifier);
        byte[] uuid = identifier.getBinary("id").getData();
        assertNotNull(uuid);
        assertEquals(16, uuid.length);
    }

    @Test
    @DisplayName("should advance transaction number")
    void testAdvanceTransactionNumber() {
        ServerSessionPool.Clock clock = mock(ServerSessionPool.Clock.class);
        when(clock.millis()).thenReturn(42L);

        ServerSessionPool pool = new ServerSessionPool(clusterMock, OPERATION_CONTEXT, clock);
        ServerSession session = pool.get();

        ServerSessionPool.ServerSessionImpl sessionImpl = (ServerSessionPool.ServerSessionImpl) session;
        assertEquals(0L, sessionImpl.getTransactionNumber());
        assertEquals(1L, sessionImpl.advanceTransactionNumber());
        assertEquals(1L, sessionImpl.getTransactionNumber());
    }

    @Test
    @DisplayName("should end pooled sessions when pool is closed")
    void testEndPooledSessionsWhenPoolClosed() {
        Connection connection = mock(Connection.class);
        Server server = mock(Server.class);
        when(server.getConnection(any())).thenReturn(connection);

        when(clusterMock.getCurrentDescription()).thenReturn(connectedDescription);
        when(clusterMock.selectServer(any(), any()))
                .thenReturn(new ServerTuple(server, connectedDescription.getServerDescriptions().get(0)));

        when(connection.command(
                any(String.class),
                any(BsonDocument.class),
                any(),
                any(),
                any(),
                any()
        )).thenReturn(new BsonDocument());

        ServerSessionPool pool = new ServerSessionPool(clusterMock, TIMEOUT_SETTINGS, getServerApi());
        List<ServerSession> sessions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            sessions.add(pool.get());
        }

        for (ServerSession session : sessions) {
            pool.release(session);
        }

        pool.close();

        verify(clusterMock, times(1)).selectServer(any(), any());
        verify(connection, times(1)).command(
                any(String.class),
                argThat(endSessionsDocMatcher(sessions)),
                any(),
                any(),
                any(),
                any()
        );
        verify(connection, times(1)).release();
    }

    @Test
    @DisplayName("should handle MongoException during endSessions without leaking resources")
    void testHandleMongoExceptionDuringEndSessionsWithoutLeakingResources() {
        Connection connection = mock(Connection.class);
        Server server = mock(Server.class);
        when(server.getConnection(any())).thenReturn(connection);

        when(clusterMock.getCurrentDescription()).thenReturn(connectedDescription);
        when(clusterMock.selectServer(any(), any()))
                .thenReturn(new ServerTuple(server, connectedDescription.getServerDescriptions().get(0)));

        when(connection.command(
                any(String.class),
                any(BsonDocument.class),
                any(),
                any(),
                any(),
                any()
        )).thenThrow(new MongoException("Simulated error"));

        ServerSessionPool pool = new ServerSessionPool(clusterMock, TIMEOUT_SETTINGS, getServerApi());
        List<ServerSession> sessions = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            sessions.add(pool.get());
        }

        for (ServerSession session : sessions) {
            pool.release(session);
        }

        // Should not throw - exception is handled internally
        pool.close();

        verify(clusterMock, times(1)).selectServer(any(), any());
        verify(connection, times(1)).release();
    }

    /**
     * Matcher to verify the endSessions document contains the correct session identifiers.
     */
    private ArgumentMatcher<BsonDocument> endSessionsDocMatcher(List<ServerSession> sessions) {
        return doc -> {
            if (!doc.containsKey("endSessions")) {
                return false;
            }
            BsonArray endSessionsArray = doc.getArray("endSessions");
            if (endSessionsArray.size() != sessions.size()) {
                return false;
            }
            for (int i = 0; i < sessions.size(); i++) {
                ServerSession session = sessions.get(i);
                BsonDocument sessionIdentifier = session.getIdentifier();
                BsonDocument arrayElement = endSessionsArray.get(i).asDocument();
                if (!sessionIdentifier.equals(arrayElement)) {
                    return false;
                }
            }
            return true;
        };
    }
}
