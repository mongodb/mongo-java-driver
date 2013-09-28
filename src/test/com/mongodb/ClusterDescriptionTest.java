package com.mongodb;

import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static com.mongodb.ClusterConnectionMode.Multiple;
import static com.mongodb.ClusterType.ReplicaSet;
import static com.mongodb.ClusterType.Unknown;
import static com.mongodb.ServerConnectionState.Connected;
import static com.mongodb.ServerConnectionState.Connecting;
import static com.mongodb.ServerType.ReplicaSetPrimary;
import static com.mongodb.util.MyAsserts.assertFalse;
import static com.mongodb.util.MyAsserts.assertTrue;
import static org.testng.Assert.assertEquals;

public class ClusterDescriptionTest {
    @Test
    public void testMode() {
        ClusterDescription description = new ClusterDescription(Multiple, Unknown, Collections.<ServerDescription>emptyList());
        assertEquals(Multiple, description.getConnectionMode());
    }

    @Test
    public void testEmptySet() {
        ClusterDescription description = new ClusterDescription(Multiple, Unknown, Collections.<ServerDescription>emptyList());
        assertTrue(description.getAll().isEmpty());
    }

    @Test
    public void testIsConnecting() throws UnknownHostException {
        ClusterDescription description =
        new ClusterDescription(Multiple, ReplicaSet, Arrays.asList(ServerDescription.builder()
                                                                                    .state(Connecting)
                                                                                    .address(new ServerAddress())
                                                                                    .type(ReplicaSetPrimary)
                                                                                    .build()));
        assertTrue(description.isConnecting());

        description = new ClusterDescription(Multiple, ReplicaSet, Arrays.asList(
                                                                                ServerDescription.builder()
                                                                                                 .state(Connected)
                                                                                                 .address(new ServerAddress())
                                                                                                 .type(ReplicaSetPrimary)
                                                                                                 .build()));
        assertFalse(description.isConnecting());
    }

    @Test
    public void testSortingOfAll() throws UnknownHostException {
        ClusterDescription description =
        new ClusterDescription(Multiple, Unknown, Arrays.asList(
                                                               ServerDescription.builder()
                                                                                .state(Connecting)
                                                                                .address(new ServerAddress("loc:27019"))
                                                                                .build(),
                                                               ServerDescription.builder()
                                                                                .state(Connecting)
                                                                                .address(new ServerAddress("loc:27018"))
                                                                                .build(),
                                                               ServerDescription.builder()
                                                                                .state(Connecting)
                                                                                .address(new ServerAddress("loc:27017"))
                                                                                .build())
        );
        Iterator<ServerDescription> iter = description.getAll().iterator();
        assertEquals(new ServerAddress("loc:27017"), iter.next().getAddress());
        assertEquals(new ServerAddress("loc:27018"), iter.next().getAddress());
        assertEquals(new ServerAddress("loc:27019"), iter.next().getAddress());
    }

    @Test
    public void testObjectOverrides() throws UnknownHostException {
        ClusterDescription description =
        new ClusterDescription(Multiple, Unknown, Arrays.asList(
                                                               ServerDescription.builder()
                                                                                .state(Connecting)
                                                                                .address(new ServerAddress("loc:27019"))
                                                                                .build(),
                                                               ServerDescription.builder()
                                                                                .state(Connecting)
                                                                                .address(new ServerAddress("loc:27018"))
                                                                                .build(),
                                                               ServerDescription.builder()
                                                                                .state(Connecting)
                                                                                .address(new ServerAddress("loc:27017"))
                                                                                .build())
        );
        ClusterDescription descriptionTwo =
        new ClusterDescription(Multiple, Unknown, Arrays.asList(
                                                               ServerDescription.builder()
                                                                                .state(Connecting)
                                                                                .address(new ServerAddress("loc:27019"))
                                                                                .build(),
                                                               ServerDescription.builder()
                                                                                .state(Connecting)
                                                                                .address(new ServerAddress("loc:27018"))
                                                                                .build(),
                                                               ServerDescription.builder()
                                                                                .state(Connecting)
                                                                                .address(new ServerAddress("loc:27017"))
                                                                                .build())
        );
        assertEquals(description, descriptionTwo);
        assertEquals(description.hashCode(), descriptionTwo.hashCode());
        assertTrue(description.toString().startsWith("ClusterDescription"));
    }
}