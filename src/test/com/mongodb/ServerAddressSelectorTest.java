package com.mongodb;

import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Collections;

import static com.mongodb.ServerConnectionState.Connected;
import static com.mongodb.ServerDescription.builder;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class ServerAddressSelectorTest {

    private ClusterDescription clusterDescription;
    private ServerDescription goodServer;
    private ServerDescription badServer;


    @Before
    public void setUp() throws UnknownHostException {
        goodServer = builder().ok(true).state(Connected).address(new ServerAddress("localhost", 27018)).build();
        badServer = builder().ok(false).state(Connected).address(new ServerAddress("localhost", 27019)).build();
        clusterDescription = new ClusterDescription(ClusterConnectionMode.Multiple, ClusterType.ReplicaSet,
                                                    asList(goodServer, badServer));
    }

    @Test
    public void shouldFindServerDescriptionForServerAddress() throws UnknownHostException {
        assertEquals(new ServerAddressSelector(new ServerAddress("localhost", 27018)).choose(clusterDescription), asList(goodServer));
    }

    @Test
    public void shouldNotFindServerDescriptionForServerAddressThatIsNotOk() throws UnknownHostException {
        assertEquals(new ServerAddressSelector(new ServerAddress("localhost", 27019)).choose(clusterDescription), Collections.emptyList());
    }

    @Test
    public void shouldNotFindServerDescriptionForServerAddressThatIsNotInClusterDescription() throws UnknownHostException {
        assertEquals(new ServerAddressSelector(new ServerAddress("localhost", 27017)).choose(clusterDescription), Collections.emptyList());
    }
}
