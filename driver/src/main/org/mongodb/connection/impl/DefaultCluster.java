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

package org.mongodb.connection.impl;

import org.mongodb.MongoInterruptedException;
import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.connection.ChangeEvent;
import org.mongodb.connection.ChangeListener;
import org.mongodb.connection.Cluster;
import org.mongodb.connection.ClusterDescription;
import org.mongodb.connection.ClusterType;
import org.mongodb.connection.ClusterableServer;
import org.mongodb.connection.ClusterableServerFactory;
import org.mongodb.connection.Connection;
import org.mongodb.connection.MongoServerSelectionFailureException;
import org.mongodb.connection.MongoTimeoutException;
import org.mongodb.connection.Server;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.ServerSelector;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.String.format;
import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

public abstract class DefaultCluster implements Cluster {

    private static final Logger LOGGER = Logger.getLogger("org.mongodb.connection");

    private final Set<ChangeListener<ClusterDescription>> changeListeners =
            Collections.newSetFromMap(new ConcurrentHashMap<ChangeListener<ClusterDescription>, Boolean>());
    private final AtomicReference<CountDownLatch> phase = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
    private final ClusterableServerFactory serverFactory;
    private final ThreadLocal<Random> random = new ThreadLocal<Random>() {
        @Override
        protected Random initialValue() {
            return new Random();
        }
    };

    private volatile boolean isClosed;
    private volatile ClusterDescription description;

    public DefaultCluster(final ClusterableServerFactory serverFactory) {
        this.serverFactory = notNull("serverFactory", serverFactory);
    }

    @Override
    public Server getServer(final ServerSelector serverSelector) {
        isTrue("open", !isClosed());

        try {
            CountDownLatch currentPhase = phase.get();
            ClusterDescription curDescription = description;
            List<ServerDescription> serverDescriptions = serverSelector.choose(curDescription);
            final TimeUnit timeUnit = TimeUnit.NANOSECONDS;
            final long endTime = System.nanoTime() + timeUnit.convert(20, TimeUnit.SECONDS); // TODO: configurable
            while (serverDescriptions.isEmpty()) {

                if (!curDescription.isConnecting()) {
                    throw new MongoServerSelectionFailureException(format("No server satisfies the selector %s", serverSelector));
                }

                final long timeout = endTime - System.nanoTime();

                LOGGER.log(Level.INFO, format("No server chosen by %s from cluster description %s. Waiting for %d ms before timing out",
                                              serverSelector, curDescription, TimeUnit.MILLISECONDS.convert(timeout, timeUnit)));

                if (!currentPhase.await(timeout, timeUnit)) {
                    throw new MongoTimeoutException(format("Timed out while waiting for a server that satisfies the selector: %s "
                                                           + "after  %d %s", serverSelector, timeout, timeUnit));
                }
                currentPhase = phase.get();
                curDescription = description;
                serverDescriptions = serverSelector.choose(curDescription);
            }
            return new WrappedServer(getServer(getRandomServer(serverDescriptions).getAddress()));
        } catch (InterruptedException e) {
            throw new MongoInterruptedException(format("Interrupted while waiting for a server that satisfies server selector %s ",
                                                       serverSelector), e);
        }
    }

    @Override
    public ClusterDescription getDescription() {
        isTrue("open", !isClosed());

        try {
            CountDownLatch currentPhase = phase.get();
            ClusterDescription curDescription = description;
            final TimeUnit timeUnit = TimeUnit.NANOSECONDS;
            final long endTime = System.nanoTime() + timeUnit.convert(20, TimeUnit.SECONDS); // TODO: configurable
            while (curDescription.getType() == ClusterType.Unknown) {

                if (!curDescription.isConnecting()) {
                    throw new MongoServerSelectionFailureException(format("Unable to determine cluster type"));
                }

                final long timeout = endTime - System.nanoTime();

                LOGGER.log(Level.FINE, format("Cluster description not yet available. Waiting for %d ms before timing out",
                                              TimeUnit.MILLISECONDS.convert(timeout, timeUnit)));

                if (!currentPhase.await(timeout, timeUnit)) {
                    throw new MongoTimeoutException(format("Timed out while waiting for the cluster description after waiting %d %s",
                                                           timeout, timeUnit));
                }
                currentPhase = phase.get();
                curDescription = description;
            }
            return curDescription;
        } catch (InterruptedException e) {
            throw new MongoInterruptedException(
                    format("Interrupted while waiting for the cluster description"), e);
        }
    }

    @Override
    public void addChangeListener(final ChangeListener<ClusterDescription> changeListener) {
        isTrue("open", !isClosed());

        changeListeners.add(changeListener);
    }

    @Override
    public void removeChangeListener(final ChangeListener<ClusterDescription> changeListener) {
        isTrue("open", !isClosed());
        isTrue("listener is not null", changeListener != null);

        changeListeners.remove(changeListener);
    }

    @Override
    public void close() {
        if (!isClosed()) {
            isClosed = true;
            serverFactory.close();
            phase.get().countDown();
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    protected abstract ClusterableServer getServer(final ServerAddress serverAddress);

    protected synchronized void updateDescription(final ClusterDescription newDescription) {
        LOGGER.log(Level.FINE, format("Updating cluster description %s and notifying all waiters", newDescription));

        description = newDescription;
        final CountDownLatch current = phase.getAndSet(new CountDownLatch(1));
        current.countDown();
    }

    // this method is necessary so that subclasses can call fireChangeEvent with the old value of description.
    protected ClusterDescription getDescriptionNoWaiting() {
        return description;
    }

    protected void fireChangeEvent(final ChangeEvent<ClusterDescription> changeEvent) {
        for (final ChangeListener<ClusterDescription> listener : changeListeners) {
            listener.stateChanged(changeEvent);
        }
    }

    private ServerDescription getRandomServer(final List<ServerDescription> serverDescriptions) {
        return serverDescriptions.get(getRandom().nextInt(serverDescriptions.size()));
    }

    protected Random getRandom() {
        return random.get();
    }

    protected ClusterableServer createServer(final ServerAddress serverAddress, final ChangeListener<ServerDescription>
            serverStateListener) {
        final ClusterableServer server = serverFactory.create(serverAddress);
        server.addChangeListener(serverStateListener);
        return server;
    }

    private static final class WrappedServer implements Server {
        private final ClusterableServer wrapped;

        public WrappedServer(final ClusterableServer server) {
            wrapped = server;
        }

        @Override
        public ServerDescription getDescription() {
            return wrapped.getDescription();
        }

        @Override
        public Connection getConnection() {
            return wrapped.getConnection();
        }

        @Override
        public AsyncServerConnection getAsyncConnection() {
            return wrapped.getAsyncConnection();
        }
    }
}
