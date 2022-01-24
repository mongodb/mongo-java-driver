/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.connection;

import com.mongodb.connection.ClusterId;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;
import com.mongodb.event.ServerClosedEvent;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.event.ServerHeartbeatFailedEvent;
import com.mongodb.event.ServerHeartbeatStartedEvent;
import com.mongodb.event.ServerHeartbeatSucceededEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerMonitorListener;
import com.mongodb.event.ServerOpeningEvent;
import com.mongodb.internal.VisibleForTesting;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;

/**
 * An implementation of a listener for all cluster-related events.  Its purpose is the following:
 *
 * 1. To ensure that cluster-related events are delivered one at a time, with happens-before semantics
 * 2. To ensure that application-provided event listener methods do not execute within critical sections of the driver
 *
 * This is done by adding all events to an unbounded blocking queue, and then publishing them from a dedicated thread by taking
 * them off the queue one at a time.
 *
 * There is an assumption that the last event that should be published is the {@link ClusterClosedEvent}.  Once that event is published,
 * the publishing thread is allowed to die.
 */
final class AsynchronousClusterEventListener implements ClusterListener, ServerListener, ServerMonitorListener {
    private final BlockingQueue<Supplier<Boolean>> eventPublishers = new LinkedBlockingQueue<>();
    private final ClusterListener clusterListener;
    private final ServerListener serverListener;
    private final ServerMonitorListener serverMonitorListener;

    private final Thread publishingThread;

    @FunctionalInterface
    private interface VoidFunction<T> {
        void apply(T t);
    }

    AsynchronousClusterEventListener(final ClusterId clusterId, final ClusterListener clusterListener, final ServerListener serverListener,
            final ServerMonitorListener serverMonitorListener) {
        this.clusterListener = notNull("clusterListener", clusterListener);
        this.serverListener = notNull("serverListener", serverListener);
        this.serverMonitorListener = notNull("serverMonitorListener", serverMonitorListener);
        publishingThread = new Thread(this::publishEvents, "cluster-event-publisher-" + clusterId.getValue());
        publishingThread.setDaemon(true);
        publishingThread.start();
    }

    @VisibleForTesting(otherwise = PRIVATE)
    Thread getPublishingThread() {
        return publishingThread;
    }

    @Override
    public void clusterOpening(final ClusterOpeningEvent event) {
        addClusterEventInvocation(clusterListener -> clusterListener.clusterOpening(event), false);
    }

    @Override
    public void clusterClosed(final ClusterClosedEvent event) {
        addClusterEventInvocation(clusterListener -> clusterListener.clusterClosed(event), true);
    }

    @Override
    public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
        addClusterEventInvocation(clusterListener -> clusterListener.clusterDescriptionChanged(event), false);
    }

    @Override
    public void serverOpening(final ServerOpeningEvent event) {
        addServerEventInvocation(serverListener -> serverListener.serverOpening(event));
    }

    @Override
    public void serverClosed(final ServerClosedEvent event) {
        addServerEventInvocation(serverListener -> serverListener.serverClosed(event));
    }

    @Override
    public void serverDescriptionChanged(final ServerDescriptionChangedEvent event) {
        addServerEventInvocation(serverListener -> serverListener.serverDescriptionChanged(event));
    }

    @Override
    public void serverHearbeatStarted(final ServerHeartbeatStartedEvent event) {
        addServerMonitorEventInvocation(serverMonitorListener -> serverMonitorListener.serverHearbeatStarted(event));
    }

    @Override
    public void serverHeartbeatSucceeded(final ServerHeartbeatSucceededEvent event) {
        addServerMonitorEventInvocation(serverMonitorListener -> serverMonitorListener.serverHeartbeatSucceeded(event));
    }

    @Override
    public void serverHeartbeatFailed(final ServerHeartbeatFailedEvent event) {
        addServerMonitorEventInvocation(serverMonitorListener -> serverMonitorListener.serverHeartbeatFailed(event));
    }

    private void addClusterEventInvocation(final VoidFunction<ClusterListener> eventPublisher, final boolean isLastEvent) {
        addEvent(() -> {
            eventPublisher.apply(clusterListener);
            return isLastEvent;
        });
    }

    private void addServerEventInvocation(final VoidFunction<ServerListener> eventPublisher) {
        addEvent(() -> {
            eventPublisher.apply(serverListener);
            return false;
        });
    }

    private void addServerMonitorEventInvocation(final VoidFunction<ServerMonitorListener> eventPublisher) {
        addEvent(() -> {
            eventPublisher.apply(serverMonitorListener);
            return false;
        });
    }

    private void addEvent(final Supplier<Boolean> supplier) {
        // protect against rogue publishers
        if (!publishingThread.isAlive()) {
            return;
        }
        eventPublishers.add(supplier);
    }

    private void publishEvents() {
        try {
            while (true) {
                Supplier<Boolean> eventPublisher = eventPublishers.take();
                try {
                    boolean isLastEvent = eventPublisher.get();
                    if (isLastEvent) {
                        break;
                    }
                } catch (Exception e) {
                    // ignore exceptions thrown from listeners
                }
            }
        } catch (InterruptedException e) {
            // ignore
        }
    }
}
