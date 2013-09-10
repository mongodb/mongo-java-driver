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

package org.mongodb.event;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A multicaster for cluster events.
 *
 * @since 3.0
 */
public class ClusterEventMulticaster implements ClusterListener {
    private Set<ClusterListener> clusterListeners = Collections.newSetFromMap(new ConcurrentHashMap<ClusterListener, Boolean>());

    /**
     * Adds the given cluster listener to the list of listeners to invoke on cluster events.
     *
     * @param clusterListener the cluster listener
     */
    public void add(final ClusterListener clusterListener) {
        clusterListeners.add(clusterListener);
    }

    /**
     * Removed the given cluster listener from the list of listeners to invoke on cluster events.
     *
     * @param clusterListener the cluster listener
     */
    public void remove(final ClusterListener clusterListener) {
        clusterListeners.remove(clusterListener);
    }

    @Override
    public void clusterOpened(final ClusterEvent event) {
        for (ClusterListener cur : clusterListeners) {
            cur.clusterOpened(event);
        }
    }

    @Override
    public void clusterClosed(final ClusterEvent event) {
        for (ClusterListener cur : clusterListeners) {
            cur.clusterClosed(event);
        }
    }

    @Override
    public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
        for (ClusterListener cur : clusterListeners) {
            cur.clusterDescriptionChanged(event);
        }
    }
}
