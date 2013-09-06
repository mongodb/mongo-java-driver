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

import org.bson.ByteBuf;
import org.mongodb.MongoException;
import org.mongodb.MongoInternalException;
import org.mongodb.diagnostics.Loggers;
import org.mongodb.management.MBeanServerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

class DefaultChannelProvider implements ChannelProvider {

    private static final Logger LOGGER = Loggers.getLogger("connection");

    private final ConcurrentPool<UsageTrackingConnection> pool;
    private final ConnectionPoolSettings settings;
    private final AtomicInteger waitQueueSize = new AtomicInteger(0);
    private final AtomicInteger generation = new AtomicInteger(0);
    private final ExecutorService sizeMaintenanceTimer;
    private final ServerAddress serverAddress;
    private final ConnectionPoolStatistics statistics;
    private final Runnable maintenanceTask;

    public DefaultChannelProvider(final ServerAddress serverAddress, final ConnectionFactory connectionFactory,
                                  final ConnectionPoolSettings settings) {
        this.serverAddress = serverAddress;
        this.settings = settings;
        pool = new ConcurrentPool<UsageTrackingConnection>(settings.getMaxSize(),
                new UsageTrackingConnectionItemFactory(connectionFactory));
        statistics = new ConnectionPoolStatistics(serverAddress, settings.getMinSize(), settings.getMaxSize(), pool);
        MBeanServerFactory.getMBeanServer().registerMBean(statistics, statistics.getObjectName());
        maintenanceTask = createMaintenanceTask();
        sizeMaintenanceTimer = createTimer();
    }

    @Override
    public Channel get() {
        return get(settings.getMaxWaitTime(MILLISECONDS), MILLISECONDS);
    }

    @Override
    public Channel get(final long timeout, final TimeUnit timeUnit) {
        try {
            if (waitQueueSize.incrementAndGet() > settings.getMaxWaitQueueSize()) {
                throw new MongoWaitQueueFullException(format("Too many threads are already waiting for a connection. "
                        + "Max number of threads (maxWaitQueueSize) of %d has been exceeded.",
                        settings.getMaxWaitQueueSize()));
            }
            UsageTrackingConnection connection = pool.get(timeout, timeUnit);
            while (shouldPrune(connection)) {
                pool.release(connection, true);
                connection = pool.get(timeout, timeUnit);
            }
            return new PooledChannel(connection);
        } finally {
            waitQueueSize.decrementAndGet();
        }
    }

    @Override
    public void close() {
        pool.close();
        if (sizeMaintenanceTimer != null) {
            sizeMaintenanceTimer.shutdownNow();
        }
        MBeanServerFactory.getMBeanServer().unregisterMBean(statistics.getObjectName());
    }

    /**
     * Gets the statistics for the connection pool.
     *
     * @return the statistics.
     */
    public ConnectionPoolStatistics getStatistics() {
        return statistics;
    }

    /**
     * Synchronously prune idle connections and ensure the minimum pool size.
     */
    public void doMaintenance() {
        if (maintenanceTask != null) {
            maintenanceTask.run();
        }
    }

    private Runnable createMaintenanceTask() {
        Runnable newMaintenanceTask = null;
        if (shouldPrune() || shouldEnsureMinSize()) {
            newMaintenanceTask = new Runnable() {
                @Override
                public synchronized void run() {
                    if (shouldPrune()) {
                        LOGGER.fine(format("Pruning pooled connections to %s", serverAddress));
                        pool.prune();
                    }
                    if (shouldEnsureMinSize()) {
                        LOGGER.fine(format("Ensuring minimum pooled connections to %s", serverAddress));
                        pool.ensureMinSize(settings.getMinSize());
                    }
                }
            };
        }
        return newMaintenanceTask;
    }

    private ExecutorService createTimer() {
        if (maintenanceTask == null) {
            return null;
        }
        else {
            ScheduledExecutorService newTimer = Executors.newSingleThreadScheduledExecutor();
            newTimer.scheduleAtFixedRate(maintenanceTask, 0, settings.getMaintenanceFrequency(MILLISECONDS), MILLISECONDS);
            return newTimer;
        }
    }

    private boolean shouldEnsureMinSize() {
        return settings.getMinSize() > 0;
    }

    private boolean shouldPrune() {
        return settings.getMaxConnectionIdleTime(MILLISECONDS) > 0 || settings.getMaxConnectionLifeTime(MILLISECONDS) > 0;
    }

    private boolean shouldPrune(final UsageTrackingConnection connection) {
        return fromPreviousGeneration(connection) || pastMaxLifeTime(connection) || pastMaxIdleTime(connection);
    }

    private boolean pastMaxIdleTime(final UsageTrackingConnection connection) {
        return expired(connection.getLastUsedAt(), System.currentTimeMillis(), settings.getMaxConnectionIdleTime(MILLISECONDS));
    }

    private boolean pastMaxLifeTime(final UsageTrackingConnection connection) {
        return expired(connection.getOpenedAt(), System.currentTimeMillis(), settings.getMaxConnectionLifeTime(MILLISECONDS));
    }

    private boolean fromPreviousGeneration(final UsageTrackingConnection connection) {
        return generation.get() > connection.getGeneration();
    }

    private boolean expired(final long startTime, final long curTime, final long maxTime) {
        return maxTime != 0 && curTime - startTime > maxTime;
    }

    /**
     * If there was a socket exception that wasn't some form of interrupted read, increment the generation count so that
     * any connections created prior will be discarded.
     *
     * @param channel the channel that generated the exception
     * @param e the exception
     */
    private void incrementGenerationOnSocketException(final Channel channel, final MongoException e) {
        if (e instanceof MongoSocketException && !(e instanceof MongoSocketInterruptedReadException)) {
            LOGGER.warning(format("Got socket exception on connection [%s] to %s. All connections to %s will be closed.",
                    channel.getId(), serverAddress, serverAddress));
            generation.incrementAndGet();
        }
    }

    private class PooledChannel implements Channel {
        private volatile UsageTrackingConnection wrapped;

        public PooledChannel(final UsageTrackingConnection wrapped) {
            this.wrapped = notNull("wrapped", wrapped);
        }

        @Override
        public void close() {
            if (wrapped != null) {
                pool.release(wrapped, wrapped.isClosed() || shouldPrune(wrapped));
                wrapped = null;
            }
        }

        @Override
        public boolean isClosed() {
            return wrapped == null || wrapped.isClosed();
        }

        @Override
        public ServerAddress getServerAddress() {
            isTrue("open", !isClosed());
            return wrapped.getServerAddress();
        }

        @Override
        public void sendMessage(final List<ByteBuf> byteBuffers) {
            isTrue("open", wrapped != null);
            try {
                wrapped.sendMessage(byteBuffers);
            } catch (MongoException e) {
                incrementGenerationOnSocketException(this, e);
                throw e;
            }
        }

        @Override
        public ResponseBuffers receiveMessage(final ChannelReceiveArgs channelReceiveArgs) {
            isTrue("open", wrapped != null);
            try {
                ResponseBuffers responseBuffers = wrapped.receiveMessage();
                if (responseBuffers.getReplyHeader().getResponseTo() != channelReceiveArgs.getResponseTo()) {
                    throw new MongoInternalException(
                            String.format("The responseTo (%d) in the response does not match the requestId (%d) in the request",
                                    responseBuffers.getReplyHeader().getResponseTo(), channelReceiveArgs.getResponseTo()));
                }

//                if (responseBuffers.getReplyHeader().getMessageLength() > channelReceiveArgs.getMaxMessageSize()) {
//                    throw new MongoInternalException(String.format("Unexpectedly large message length of %d exceeds maximum of %d",
//                            responseBuffers.getReplyHeader().getMessageLength(), channelReceiveArgs.getMaxMessageSize()));
//                }

                return responseBuffers;
            } catch (MongoException e) {
                incrementGenerationOnSocketException(this, e);
                throw e;
            }
        }

        @Override
        public void sendMessageAsync(final List<ByteBuf> byteBuffers, final SingleResultCallback<Void> callback) {
            isTrue("open", wrapped != null);
            wrapped.sendMessageAsync(byteBuffers, callback);      // TODO: handle async exceptions
        }

        @Override
        public void receiveMessageAsync(final ChannelReceiveArgs channelReceiveArgs, final SingleResultCallback<ResponseBuffers> callback) {
            isTrue("open", wrapped != null);
            wrapped.receiveMessageAsync(callback);                // TODO: handle async exceptions
        }

        @Override
        public String getId() {
            return wrapped.getId();
        }
    }

    private class UsageTrackingConnectionItemFactory implements ConcurrentPool.ItemFactory<UsageTrackingConnection> {
        private ConnectionFactory connectionFactory;

        public UsageTrackingConnectionItemFactory(final ConnectionFactory connectionFactory) {
            this.connectionFactory = connectionFactory;
        }

        @Override
        public UsageTrackingConnection create() {
            UsageTrackingConnection connection = new UsageTrackingConnection(connectionFactory.create(serverAddress), generation.get());
            LOGGER.info(format("Opened connection [%s] to %s", connection.getId(), serverAddress));
            return connection;
        }

        @Override
        public void close(final UsageTrackingConnection connection) {
            String reason;
            if (fromPreviousGeneration(connection)) {
                reason = "there was a socket exception raised on another connection from this pool";
            }
            else if (pastMaxLifeTime(connection)) {
                reason = "it is past its maximum allowed life time";
            }
            else if (pastMaxIdleTime(connection)) {
                reason = "it is past its maximum allowed idle time";
            }
            else {
                reason = "the pool has been closed";
            }
            connection.close();
            LOGGER.info(format("Closed connection [%s] to %s because %s.", connection.getId(), serverAddress, reason));
        }

        @Override
        public boolean shouldPrune(final UsageTrackingConnection usageTrackingConnection) {
            return DefaultChannelProvider.this.shouldPrune(usageTrackingConnection);
        }
    }
}