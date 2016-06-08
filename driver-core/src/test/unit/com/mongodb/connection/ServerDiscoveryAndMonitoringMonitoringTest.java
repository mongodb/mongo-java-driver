/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mongodb.connection;

import com.mongodb.ServerAddress;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterOpeningEvent;
import com.mongodb.event.ServerClosedEvent;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerOpeningEvent;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

// See https://github.com/mongodb/specifications/tree/master/source/...
@RunWith(Parameterized.class)
public class ServerDiscoveryAndMonitoringMonitoringTest extends AbstractServerDiscoveryAndMonitoringTest {
    private final TestClusterListener clusterListener = new TestClusterListener();
    private final TestServerListenerFactory serverListenerFactory = new TestServerListenerFactory();

    public ServerDiscoveryAndMonitoringMonitoringTest(final String description, final BsonDocument definition) {
        super(definition);
        init(serverListenerFactory, clusterListener);
    }

    @Test
    public void shouldPassAllOutcomes() {
        for (BsonValue phase : getDefinition().getArray("phases")) {
            for (BsonValue response : phase.asDocument().getArray("responses")) {
                applyResponse(response.asArray());
            }
            BsonDocument outcome = phase.asDocument().getDocument("outcome");
            assertEvents(outcome.getArray("events"));
        }
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        return data("/server-discovery-and-monitoring-monitoring");
    }

    private void assertEvents(final BsonArray events) {
        Iterator<ClusterDescriptionChangedEvent> clusterDescriptionChangedEventIterator =
                clusterListener.getClusterDescriptionChangedEvents().iterator();
        for (BsonValue eventValue : events) {
            BsonDocument eventDocument = eventValue.asDocument();
            if (eventDocument.containsKey("topology_opening_event")) {
                ClusterOpeningEvent event = clusterListener.getClusterOpeningEvent();
                assertNotNull("event", event);
                assertEquals("clusterId", getCluster().getClusterId(), event.getClusterId());
            } else if (eventDocument.containsKey("topology_description_changed_event")) {
                ClusterDescriptionChangedEvent event = clusterDescriptionChangedEventIterator.next();
                assertNotNull("event", event);
                assertEquals(getCluster().getClusterId(), event.getClusterId());
                BsonDocument topologyDescriptionChangedEventDocument = eventDocument.getDocument("topology_description_changed_event");
                assertEqualClusterDescriptions(createClusterDescriptionFromClusterDescriptionDocument(
                        topologyDescriptionChangedEventDocument.getDocument("previousDescription")),
                        event.getPreviousDescription());
                BsonDocument newDescription = topologyDescriptionChangedEventDocument.getDocument("newDescription");
                assertEqualClusterDescriptions(createClusterDescriptionFromClusterDescriptionDocument(newDescription),
                        event.getNewDescription());
                if (newDescription.getString("topologyType").getValue().equals("Single")) {
                    assertEquals(SingleServerCluster.class, getCluster().getClass());
                } else {
                    assertEquals(MultiServerCluster.class, getCluster().getClass());
                }

            } else if (eventDocument.containsKey("server_opening_event")) {
                BsonDocument serverOpeningEventDocument = eventDocument.getDocument("server_opening_event");
                ServerAddress serverAddress = new ServerAddress(serverOpeningEventDocument.getString("address").getValue());
                TestServerListener serverListener = serverListenerFactory.getListener(serverAddress);
                assertNotNull("serverListener", serverListener);
                ServerOpeningEvent event = serverListener.getServerOpeningEvent();
                assertNotNull("event", event);
                assertEquals("serverId", new ServerId(getCluster().getClusterId(), serverAddress), event.getServerId());
            } else if (eventDocument.containsKey("server_closed_event")) {
                BsonDocument serverClosedEventDocument = eventDocument.getDocument("server_closed_event");
                ServerAddress serverAddress = new ServerAddress(serverClosedEventDocument.getString("address").getValue());
                TestServerListener serverListener = serverListenerFactory.getListener(serverAddress);
                assertNotNull("serverListener", serverListener);
                ServerClosedEvent event = serverListener.getServerClosedEvent();
                assertNotNull("event", event);
                assertEquals("serverId", new ServerId(getCluster().getClusterId(), serverAddress), event.getServerId());
            } else if (eventDocument.containsKey("server_description_changed_event")) {
                BsonDocument serverDescriptionChangedEventDocument = eventDocument.getDocument("server_description_changed_event");
                ServerAddress serverAddress = new ServerAddress(serverDescriptionChangedEventDocument.getString("address").getValue());
                TestServerListener serverListener = serverListenerFactory.getListener(serverAddress);
                assertNotNull("serverListener", serverListener);
                assertEquals("serverDescriptionChangedEvents size", 1, serverListener.getServerDescriptionChangedEvents().size());
                ServerDescriptionChangedEvent event = serverListener.getServerDescriptionChangedEvents().get(0);
                assertNotNull("event", event);
                assertEquals("serverId", new ServerId(getCluster().getClusterId(), serverAddress), event.getServerId());
                assertEqualServerDescriptions(createServerDescriptionFromServerDescriptionDocument(serverDescriptionChangedEventDocument
                                .getDocument("previousDescription")),
                        event.getPreviousDescription());
                assertEqualServerDescriptions(createServerDescriptionFromServerDescriptionDocument(serverDescriptionChangedEventDocument
                                .getDocument("newDescription")),
                        event.getNewDescription());
            } else {
                throw new IllegalArgumentException("Unsupported event type: " + eventDocument.keySet().iterator().next());
            }
        }

        assertFalse(clusterDescriptionChangedEventIterator.hasNext());
    }

    private void assertEqualClusterDescriptions(final ClusterDescription expected, final ClusterDescription actual) {
        assertEquals(expected.getType(), actual.getType());
        assertEquals(expected.getAll().size(), actual.getAll().size());
        for (ServerDescription curExpected: expected.getAll()) {
            ServerDescription curActual = getByServerAddress(curExpected.getAddress(), actual.getAll());
            assertNotNull(curActual);
            assertEqualServerDescriptions(curExpected, curActual);
        }
    }

    private ServerDescription getByServerAddress(final ServerAddress serverAddress, final Set<ServerDescription> serverDescriptions) {
        for (ServerDescription cur: serverDescriptions) {
            if (cur.getAddress().equals(serverAddress)) {
                return cur;
            }
        }
        return null;
    }

    private void assertEqualServerDescriptions(final ServerDescription expected, final ServerDescription actual) {
        assertEquals("address", expected.getAddress(), actual.getAddress());
        assertEquals("ok", expected.isOk(), actual.isOk());
        assertEquals("type", expected.getType(), actual.getType());
        assertEquals("state", expected.getState(), actual.getState());
        assertEquals("setName", expected.getSetName(), actual.getSetName());
        assertEquals("primary", expected.getPrimary(), actual.getPrimary());
        assertEquals("hosts", expected.getHosts(), actual.getHosts());
        assertEquals("arbiters", expected.getArbiters(), actual.getArbiters());
        assertEquals("passives", expected.getPassives(), actual.getPassives());
    }

    private ClusterDescription createClusterDescriptionFromClusterDescriptionDocument(final BsonDocument clusterDescriptionDocument) {
        List<ServerDescription> serverDescriptions = new ArrayList<ServerDescription>();
        for (BsonValue cur : clusterDescriptionDocument.getArray("servers"))  {
            serverDescriptions.add(createServerDescriptionFromServerDescriptionDocument(cur.asDocument()));
        }
        return new ClusterDescription(getCluster().getSettings().getMode(),
                getClusterType(clusterDescriptionDocument.getString("topologyType").getValue(), serverDescriptions),
                serverDescriptions);
    }

    private ServerDescription createServerDescriptionFromServerDescriptionDocument(final BsonDocument serverDescriptionDocument) {
        ServerType serverType = getServerType(serverDescriptionDocument.getString("type").getValue());
        return ServerDescription.builder()
                .address(new ServerAddress(serverDescriptionDocument.getString("address").getValue()))
                .ok(serverType == ServerType.UNKNOWN ? false : true)
                .state(serverType == ServerType.UNKNOWN ? CONNECTING : CONNECTED)
                .type(serverType)
                .setName(serverDescriptionDocument.containsKey("setName")
                        ? serverDescriptionDocument.getString("setName").getValue()
                        : null)
                .primary(serverDescriptionDocument.containsKey("primary")
                        ? serverDescriptionDocument.getString("primary").getValue() : null)
                .hosts(getHostNamesSet(serverDescriptionDocument, "hosts"))
                .arbiters(getHostNamesSet(serverDescriptionDocument, "arbiters"))
                .passives(getHostNamesSet(serverDescriptionDocument, "passives"))
                .version(serverType == ServerType.UNKNOWN ? new ServerVersion() : new ServerVersion(2, 6))
                .build();
    }

    private Set<String> getHostNamesSet(final BsonDocument serverDescriptionDocument, final String fieldName) {
        Set<String> hostsSet = new HashSet<String>();
        for (BsonValue cur : serverDescriptionDocument.getArray(fieldName)) {
            hostsSet.add(cur.asString().getValue());
        }
        return hostsSet;
    }

    private static class TestServerListenerFactory implements ServerListenerFactory {
        private final Map<ServerAddress, TestServerListener> serverAddressServerListenerMap =
                new HashMap<ServerAddress, TestServerListener>();

        @Override
        public ServerListener create(final ServerAddress serverAddress) {
            TestServerListener serverListener = new TestServerListener();
            serverAddressServerListenerMap.put(serverAddress, serverListener);
            return serverListener;
        }

        TestServerListener getListener(final ServerAddress serverAddress) {
            return serverAddressServerListenerMap.get(serverAddress);
        }
    }
}
