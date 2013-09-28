package com.mongodb;

import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterConnectionMode.Multiple;
import static com.mongodb.ClusterType.ReplicaSet;
import static com.mongodb.ServerConnectionState.Connected;
import static com.mongodb.util.MyAsserts.assertNotEquals;
import static org.testng.Assert.assertEquals;

public class ReadPreferenceServerSelectorTest {
    @Test
    public void testAll() throws UnknownHostException {
        ReadPreferenceServerSelector selector = new ReadPreferenceServerSelector(ReadPreference.primary());

        assertEquals(ReadPreference.primary(), selector.getReadPreference());

        assertEquals(new ReadPreferenceServerSelector(ReadPreference.primary()), selector);
        assertNotEquals(new ReadPreferenceServerSelector(ReadPreference.secondary()), selector);
        assertNotEquals(new Object(), selector);

        assertEquals(new ReadPreferenceServerSelector(ReadPreference.primary()).hashCode(), selector.hashCode());

        assertEquals("ReadPreferenceServerSelector{readPreference=primary}", selector.toString());

        final ServerDescription primary = ServerDescription.builder()
                                                           .state(Connected)
                                                           .address(new ServerAddress())
                                                           .ok(true)
                                                           .type(ServerType.ReplicaSetPrimary)
                                                           .build();
        assertEquals(Arrays.asList(primary), selector.choose(new ClusterDescription(Multiple, ReplicaSet, Arrays.asList(primary))));
    }

    @Test
    public void testChaining() throws UnknownHostException {
        ReadPreferenceServerSelector selector = new ReadPreferenceServerSelector(ReadPreference.secondary());
        final ServerDescription primary = ServerDescription.builder()
                                                           .state(Connected)
                                                           .address(new ServerAddress())
                                                           .ok(true)
                                                           .type(ServerType.ReplicaSetPrimary)
                                                           .averagePingTime(1, TimeUnit.MILLISECONDS)
                                                           .build();
        final ServerDescription secondaryOne = ServerDescription.builder()
                                                                .state(Connected)
                                                                .address(new ServerAddress("localhost:27018"))
                                                                .ok(true)
                                                                .type(ServerType.ReplicaSetSecondary)
                                                                .averagePingTime(2, TimeUnit.MILLISECONDS)
                                                                .build();
        final ServerDescription secondaryTwo = ServerDescription.builder()
                                                                .state(Connected)
                                                                .address(new ServerAddress("localhost:27019"))
                                                                .ok(true)
                                                                .type(ServerType.ReplicaSetSecondary)
                                                                .averagePingTime(20, TimeUnit.MILLISECONDS)
                                                                .build();
        assertEquals(Arrays.asList(secondaryOne),
                     selector.choose(
                                    new ClusterDescription(Multiple, ReplicaSet,
                                                           Arrays.asList(primary, secondaryOne, secondaryTwo))));

    }
}
