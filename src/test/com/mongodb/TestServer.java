package com.mongodb;

import static com.mongodb.ServerConnectionState.Connecting;

public class TestServer implements ClusterableServer {
    private ChangeListener<ServerDescription> changeListener;
    private ServerDescription description;
    private boolean isClosed;
    private ServerAddress serverAddress;

    public TestServer(final ServerAddress serverAddress) {
        this.serverAddress = serverAddress;
        invalidate();
    }

    public void sendNotification(final ServerDescription newDescription) {
        ServerDescription currentDescription = description;
        description = newDescription;
        if (changeListener != null) {
            changeListener.stateChanged(new ChangeEvent<ServerDescription>(currentDescription, newDescription));
        }
    }

    public void addChangeListener(final ChangeListener<ServerDescription> newChangeListener) {
        this.changeListener = newChangeListener;
    }

    public void invalidate() {
        description = ServerDescription.builder().state(Connecting).address(serverAddress).build();
    }

    public void close() {
        isClosed = true;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public ServerDescription getDescription() {
        return description;
    }
}
