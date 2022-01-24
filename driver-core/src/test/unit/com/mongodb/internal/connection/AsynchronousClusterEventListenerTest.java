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

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;
import com.mongodb.event.ServerClosedEvent;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.event.ServerHeartbeatFailedEvent;
import com.mongodb.event.ServerHeartbeatStartedEvent;
import com.mongodb.event.ServerHeartbeatSucceededEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerMonitorListener;
import com.mongodb.event.ServerOpeningEvent;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.connection.ServerType.STANDALONE;
import static com.mongodb.connection.ServerType.UNKNOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;

class AsynchronousClusterEventListenerTest {
    @Test
    public void testEventsPublished() throws InterruptedException {
        AllClusterEventListener targetListener = new AllClusterEventListener();
        ClusterId clusterId = new ClusterId();
        ServerId serverId = new ServerId(clusterId, new ServerAddress());
        ConnectionId connectionId = new ConnectionId(serverId);

        AsynchronousClusterEventListener listener = new AsynchronousClusterEventListener(clusterId, targetListener, targetListener,
                targetListener);

        ClusterOpeningEvent clusterOpeningEvent = new ClusterOpeningEvent(clusterId);
        listener.clusterOpening(clusterOpeningEvent);
        assertEquals(clusterOpeningEvent, targetListener.take());

        ClusterDescriptionChangedEvent clusterDescriptionChangedEvent = new ClusterDescriptionChangedEvent(clusterId,
                new ClusterDescription(ClusterConnectionMode.SINGLE, ClusterType.STANDALONE, Collections.emptyList()),
                new ClusterDescription(ClusterConnectionMode.SINGLE, ClusterType.STANDALONE, Collections.emptyList()));
        listener.clusterDescriptionChanged(clusterDescriptionChangedEvent);
        assertEquals(clusterDescriptionChangedEvent, targetListener.take());

        ServerHeartbeatStartedEvent serverHeartbeatStartedEvent = new ServerHeartbeatStartedEvent(connectionId);
        listener.serverHearbeatStarted(serverHeartbeatStartedEvent);
        assertEquals(serverHeartbeatStartedEvent, targetListener.take());

        ServerHeartbeatSucceededEvent serverHeartbeatSucceededEvent = new ServerHeartbeatSucceededEvent(connectionId, new BsonDocument(),
                1, true);
        listener.serverHeartbeatSucceeded(serverHeartbeatSucceededEvent);
        assertEquals(serverHeartbeatSucceededEvent, targetListener.take());

        ServerHeartbeatFailedEvent serverHeartbeatFailedEvent = new ServerHeartbeatFailedEvent(connectionId, 1, true, new IOException());
        listener.serverHeartbeatFailed(serverHeartbeatFailedEvent);
        assertEquals(serverHeartbeatFailedEvent, targetListener.take());

        ServerOpeningEvent serverOpeningEvent = new ServerOpeningEvent(serverId);
        listener.serverOpening(serverOpeningEvent);
        assertEquals(serverOpeningEvent, targetListener.take());

        ServerDescriptionChangedEvent serverDescriptionChangedEvent = new ServerDescriptionChangedEvent(serverId,
                ServerDescription.builder().address(new ServerAddress()).type(UNKNOWN).state(CONNECTING).build(),
                ServerDescription.builder().address(new ServerAddress()).type(STANDALONE).state(CONNECTED).build());
        listener.serverDescriptionChanged(serverDescriptionChangedEvent);
        assertEquals(serverDescriptionChangedEvent, targetListener.take());

        ServerClosedEvent serverClosedEvent = new ServerClosedEvent(serverId);
        listener.serverClosed(serverClosedEvent);
        assertEquals(serverClosedEvent, targetListener.take());

        ClusterClosedEvent clusterClosedEvent = new ClusterClosedEvent(clusterId);
        listener.clusterClosed(clusterClosedEvent);
        assertEquals(clusterClosedEvent, targetListener.take());

        // The thread should die after publishing the ClusterClosedEvent
        listener.getPublishingThread().join(5000);
    }

    private static final class AllClusterEventListener implements ClusterListener, ServerListener, ServerMonitorListener {
        private final BlockingQueue<Object> lastEvent = new ArrayBlockingQueue<>(1);

        Object take() throws InterruptedException {
            return lastEvent.poll(5, TimeUnit.SECONDS);
        }

        void addEvent(final Object event) {
            try {
                lastEvent.put(event);
            } catch (InterruptedException e) {
                // ignore
            }
        }

        @Override
        public void clusterOpening(final ClusterOpeningEvent event) {
            addEvent(event);
        }


        @Override
        public void clusterClosed(final ClusterClosedEvent event) {
            addEvent(event);
        }

        @Override
        public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
            addEvent(event);
        }

        @Override
        public void serverOpening(final ServerOpeningEvent event) {
            addEvent(event);
        }

        @Override
        public void serverClosed(final ServerClosedEvent event) {
            addEvent(event);
        }

        @Override
        public void serverDescriptionChanged(final ServerDescriptionChangedEvent event) {
            addEvent(event);
        }

        @Override
        public void serverHearbeatStarted(final ServerHeartbeatStartedEvent event) {
            addEvent(event);
        }

        @Override
        public void serverHeartbeatSucceeded(final ServerHeartbeatSucceededEvent event) {
            addEvent(event);
        }

        @Override
        public void serverHeartbeatFailed(final ServerHeartbeatFailedEvent event) {
            addEvent(event);
        }
    }
}
