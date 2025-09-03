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

package com.mongodb.event;

import java.util.EventListener;

/**
 * A listener for cluster-related events.
 * <p>
 * All events received by {@link ClusterListener}, {@link ServerListener},
 * {@link ServerMonitorListener} are totally ordered (and the event order implies the happens-before order), provided that the listeners
 * are not shared by different {@code MongoClient}s. This guarantee holds even if you have a single class implementing
 * all of {@link ClusterListener}, {@link ServerListener}, {@link ServerMonitorListener}. However, this guarantee does not mean that
 * implementations automatically do not need to synchronize memory accesses to prevent data races.
 * For example, if data that the listener collects in memory is accessed outside of the normal execution of the listener
 * by the {@code MongoClient}, then reading and writing actions must be synchronized.
 * </p>
 * @see ServerListener
 * @see ServerMonitorListener
 * @since 3.3
 */
public interface ClusterListener extends EventListener {
    /**
     * Invoked when a cluster is opened.
     *
     * @param event the event
     */
    default void clusterOpening(ClusterOpeningEvent event) {
    }

    /**
     * Invoked when a cluster is closed.
     *
     * @param event the event
     */
    default void clusterClosed(ClusterClosedEvent event) {
    }

    /**
     * Invoked when a cluster description changes.
     *
     * @param event the event
     */
    default void clusterDescriptionChanged(ClusterDescriptionChangedEvent event) {
    }
}
