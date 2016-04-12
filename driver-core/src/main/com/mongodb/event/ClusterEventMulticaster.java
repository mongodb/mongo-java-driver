/*
 * Copyright 2008-2016 MongoDB, Inc.
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

import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;

/**
 * A multicaster for cluster events.
 *
 * @since 3.3
 */
public final class ClusterEventMulticaster implements ClusterListener {
    private static final Logger LOGGER = Loggers.getLogger("cluster.event");

    private final List<ClusterListener> clusterListeners;

    /**
     * Construct an instance with the given list of cluster listeners
     *
     * @param clusterListeners the non-null list of cluster listeners, none of which may be null
     */
    public ClusterEventMulticaster(final List<ClusterListener> clusterListeners) {
        notNull("clusterListeners", clusterListeners);
        isTrue("All ClusterListener instances are non-null", !clusterListeners.contains(null));
        this.clusterListeners = new ArrayList<ClusterListener>(clusterListeners);
    }

    /**
     * Gets the cluster listeners.
     *
     * @return the cluster listeners
     */
    public List<ClusterListener> getClusterListeners() {
        return Collections.unmodifiableList(clusterListeners);
    }

    @Override
    public void clusterOpening(final ClusterOpeningEvent event) {
        for (final ClusterListener cur : clusterListeners) {
            try {
                cur.clusterOpening(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising cluster opening event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void clusterClosed(final ClusterClosedEvent event) {
        for (final ClusterListener cur : clusterListeners) {
            try {
                cur.clusterClosed(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising cluster closed event to listener %s", cur), e);
                }

            }
        }
    }

    @Override
    public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
        for (final ClusterListener cur : clusterListeners) {
            try {
                cur.clusterDescriptionChanged(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising cluster description changed event to listener %s", cur), e);
                }
            }
        }
    }
}
