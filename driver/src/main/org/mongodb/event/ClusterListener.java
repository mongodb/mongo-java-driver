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

import java.util.EventListener;

/**
 * A listener for cluster-related events.
 *
 * @since 3.0
 */
public interface ClusterListener extends EventListener {
    /**
     * Invoked when a cluster is opened.
     *
     * @param event the event
     */
    void clusterOpened(ClusterEvent event);

    /**
     * Invoked when a cluster is closed.
     *
     * @param event the event
     */
    void clusterClosed(ClusterEvent event);

    /**
     * Invoked when a cluster description changes.
     *
     * @param event the event
     */
    void clusterDescriptionChanged(ClusterDescriptionChangedEvent event);
}
