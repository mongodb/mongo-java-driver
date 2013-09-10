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

import org.mongodb.connection.ClusterDescription;

/**
 * An event signifying that the cluster description has changed.
 *
 * @since 3.0
 */
public class ClusterDescriptionChangedEvent extends ClusterEvent {
    private final ClusterDescription clusterDescription;

    /**
     * Constructs a new instance of the event.
     *
     * @param clusterId the cluster id
     * @param clusterDescription the cluster description
     */
    public ClusterDescriptionChangedEvent(final String clusterId, final ClusterDescription clusterDescription) {
        super(clusterId);
        this.clusterDescription = clusterDescription;
    }

    /**
     * Gets the new cluster description.
     *
     * @return the cluster description
     */
    public ClusterDescription getClusterDescription() {
        return clusterDescription;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ClusterDescriptionChangedEvent that = (ClusterDescriptionChangedEvent) o;

        if (!getClusterId().equals(that.getClusterId())) {
            return false;
        }
        if (!clusterDescription.equals(that.clusterDescription)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return clusterDescription.hashCode();
    }
}
