/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb;

/**
 * A cluster-related event.
 */
class ClusterEvent {
    private final String clusterId;

    /**
     * Constructs a new instance of the event.
     *
     * @param clusterId the cluster id
     */
    public ClusterEvent(final String clusterId) {
        this.clusterId = clusterId;
    }

    /**
     * Gets the cluster id associated with this event.
     *
     * @return the cluster id
     */
    public String getClusterId() {
        return clusterId;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ClusterEvent that = (ClusterEvent) o;

        if (!clusterId.equals(that.clusterId)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return clusterId.hashCode();
    }
}
