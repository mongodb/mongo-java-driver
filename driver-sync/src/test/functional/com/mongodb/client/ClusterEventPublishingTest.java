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

package com.mongodb.client;

import com.mongodb.ClusterFixture;
import com.mongodb.connection.ClusterType;
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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

public class ClusterEventPublishingTest {

    @Test
    public void shouldPublishExpectedEvents() throws InterruptedException {
        assumeFalse(ClusterFixture.isLoadBalanced());

        AllClusterEventListener clusterEventListenerOne = new AllClusterEventListener();
        AllClusterEventListener clusterEventListenerTwo = new AllClusterEventListener();

        MongoClient client = MongoClients.create(
                getMongoClientSettingsBuilder()
                        .applyToClusterSettings(builder -> builder
                                .addClusterListener(clusterEventListenerOne)
                                .addClusterListener(clusterEventListenerTwo))
                        .applyToServerSettings(builder -> builder
                                .heartbeatFrequency(1, TimeUnit.MILLISECONDS)
                                .addServerListener(clusterEventListenerOne)
                                .addServerListener(clusterEventListenerTwo)
                                .addServerMonitorListener(clusterEventListenerOne)
                                .addServerMonitorListener(clusterEventListenerTwo))
                        .build());

        assertTrue(clusterEventListenerOne.waitUntilConnected());
        assertTrue(clusterEventListenerTwo.waitUntilConnected());

        assertTrue(clusterEventListenerOne.waitUntilHeartbeat());
        assertTrue(clusterEventListenerTwo.waitUntilHeartbeat());

        client.close();

        assertTrue(clusterEventListenerOne.waitUntilDisconnected());
        assertTrue(clusterEventListenerTwo.waitUntilDisconnected());

        assertEquals(clusterEventListenerOne.getEvents().size(), clusterEventListenerTwo.getEvents().size());
        assertEvents(clusterEventListenerOne);
        assertEvents(clusterEventListenerTwo);
    }

    private void assertEvents(final AllClusterEventListener clusterEventListener) {
        assertTrue(clusterEventListener.hasEventOfType(ClusterOpeningEvent.class));
        assertTrue(clusterEventListener.hasEventOfType(ClusterDescriptionChangedEvent.class));
        assertTrue(clusterEventListener.hasEventOfType(ClusterClosedEvent.class));

        assertTrue(clusterEventListener.hasEventOfType(ServerOpeningEvent.class));
        assertTrue(clusterEventListener.hasEventOfType(ServerClosedEvent.class));
        assertTrue(clusterEventListener.hasEventOfType(ServerDescriptionChangedEvent.class));

        assertTrue(clusterEventListener.hasEventOfType(ServerHeartbeatStartedEvent.class));
    }

    private static final class AllClusterEventListener implements ClusterListener, ServerListener, ServerMonitorListener {
        private final List<Object> events = new ArrayList<>();
        private final CountDownLatch connectedLatch = new CountDownLatch(1);
        private final CountDownLatch heartbeatLatch = new CountDownLatch(1);
        private final CountDownLatch disconnectedLatch = new CountDownLatch(1);

        public List<Object> getEvents() {
            return events;
        }

        public boolean hasEventOfType(final Class<?> eventClass) {
            return events.stream().anyMatch(event -> event.getClass().equals(eventClass));
        }

        public boolean waitUntilConnected() throws InterruptedException {
            return connectedLatch.await(5, TimeUnit.SECONDS);
        }

        public boolean waitUntilHeartbeat() throws InterruptedException {
            return heartbeatLatch.await(5, TimeUnit.SECONDS);
        }

        public boolean waitUntilDisconnected() throws InterruptedException {
            return disconnectedLatch.await(5, TimeUnit.SECONDS);
        }

        @Override
        public void clusterOpening(final ClusterOpeningEvent event) {
            events.add(event);
        }

        @Override
        public void clusterClosed(final ClusterClosedEvent event) {
            events.add(event);
            disconnectedLatch.countDown();
        }

        @Override
        public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
            events.add(event);
            if (event.getNewDescription().getType() != ClusterType.UNKNOWN) {
                connectedLatch.countDown();
            }
        }

        @Override
        public void serverOpening(final ServerOpeningEvent event) {
            events.add(event);
        }

        @Override
        public void serverClosed(final ServerClosedEvent event) {
            events.add(event);
        }

        @Override
        public void serverDescriptionChanged(final ServerDescriptionChangedEvent event) {
            events.add(event);
        }

        @Override
        public void serverHeartbeatStarted(final ServerHeartbeatStartedEvent event) {
            events.add(event);
            heartbeatLatch.countDown();
        }

        @Override
        public void serverHeartbeatSucceeded(final ServerHeartbeatSucceededEvent event) {
            events.add(event);
        }

        @Override
        public void serverHeartbeatFailed(final ServerHeartbeatFailedEvent event) {
            events.add(event);
        }
    }
}
