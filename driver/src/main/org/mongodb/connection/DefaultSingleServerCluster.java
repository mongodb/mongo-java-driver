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

import org.mongodb.MongoClientOptions;
import org.mongodb.MongoCredential;
import org.mongodb.MongoException;
import org.mongodb.MongoInterruptedException;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

class DefaultSingleServerCluster implements SingleServerCluster {
    private final Server server;
    private final BufferPool<ByteBuffer> bufferPool;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Phaser serverAvailable = new Phaser(1);
    private volatile boolean isClosed;

    public DefaultSingleServerCluster(final ServerAddress serverAddress, final List<MongoCredential> credentialList,
                                      final MongoClientOptions options, final BufferPool<ByteBuffer> bufferPool,
                                      final ServerFactory serverFactory) {
        notNull("serverAddress", serverAddress);
        notNull("options", options);
        notNull("bufferPool", bufferPool);
        notNull("serverFactory", serverFactory);

        this.bufferPool = bufferPool;
        scheduledExecutorService = Executors.newScheduledThreadPool(3);  // TODO: configurable
        this.server = serverFactory.create(serverAddress, credentialList, options, scheduledExecutorService, bufferPool);
        server.addChangeListener(new ServerStateListener() {
            @Override
            public void notify(final ServerDescription serverDescription) {
                serverAvailable.arrive();
            }

            @Override
            public void notify(final MongoException e) {
            }
        });
    }

    @Override
    public Server getServer(final ServerSelector serverSelector) {
        isTrue("open", !isClosed());

        if (server.getDescription().isOk()) {
            return server;
        }

        try {
            serverAvailable.awaitAdvanceInterruptibly(serverAvailable.getPhase(), 20, TimeUnit.SECONDS); // TODO: configurable
        } catch (InterruptedException e) {
            throw new MongoInterruptedException("Interrupted while waiting for server to become available", e);
        } catch (TimeoutException e) {
            throw new MongoTimeoutException("Interrupted while waiting for server to become available", e);
        }

        return server;
    }

    @Override
    public Server getServer(final ServerAddress serverAddress) {
        isTrue("open", !isClosed());

        return server;
    }

    @Override
    public Set<ServerAddress> getAllServerAddresses() {
        isTrue("open", !isClosed());

        return Collections.singleton(server.getServerAddress());
    }

    @Override
    public BufferPool<ByteBuffer> getBufferPool() {
        isTrue("open", !isClosed());

        return bufferPool;
    }

    @Override
    public void close() {
        if (!isClosed()) {
            isClosed = true;
            server.close();
            scheduledExecutorService.shutdownNow();
            serverAvailable.forceTermination();
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public ServerDescription getDescription() {
        return server.getDescription();
    }
}
