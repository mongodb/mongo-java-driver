package com.mongodb;

import java.util.HashMap;
import java.util.Map;

public class TestClusterableServerFactory implements ClusterableServerFactory {
    private Map<ServerAddress, TestServer> addressToServerMap = new HashMap<ServerAddress, TestServer>();

    public ClusterableServer create(final ServerAddress serverAddress) {
        addressToServerMap.put(serverAddress, new TestServer(serverAddress));
        return addressToServerMap.get(serverAddress);
    }

    public void close() {

    }
    public TestServer getServer(final ServerAddress serverAddress) {
        return addressToServerMap.get(serverAddress);
    }

}

