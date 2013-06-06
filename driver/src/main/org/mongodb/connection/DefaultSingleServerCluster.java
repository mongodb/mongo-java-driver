/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.connection;

import java.util.Arrays;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.connection.ClusterConnectionMode.Direct;

/**
 * This class needs to be final because we are leaking a reference to "this" from the constructor
 */
public final class DefaultSingleServerCluster extends DefaultCluster {
    private final ClusterableServer server;

    public DefaultSingleServerCluster(final ServerAddress serverAddress, final ClusterableServerFactory serverFactory) {
        super(serverFactory);
        notNull("serverAddress", serverAddress);

        // synchronized in the constructor because the change listener is re-entrant to this instance.
        // In other words, we are leaking a reference to "this" from the constructor.
        synchronized (this) {
            this.server = createServer(serverAddress, new ChangeListener<ServerDescription>() {
                @Override
                public void stateChanged(final ChangeEvent<ServerDescription> event) {
                    updateDescription(event.getNewValue());
                }

            });
            updateDescription();
        }
    }

    private synchronized void updateDescription(final ServerDescription serverDescription) {
        updateDescription(new ClusterDescription(Arrays.asList(serverDescription), Direct));
    }

    private void updateDescription() {
        updateDescription(server.getDescription());
    }

    @Override
    protected ClusterableServer getServer(final ServerAddress serverAddress) {
        isTrue("open", !isClosed());

        return server;
    }

    @Override
    public void close() {
        if (!isClosed()) {
            server.close();
            super.close();
        }
    }
}
