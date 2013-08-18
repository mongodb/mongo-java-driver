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

import org.mongodb.annotations.Immutable;

import java.util.Collections;
import java.util.List;

import static org.mongodb.assertions.Assertions.notNull;

/**
 * Settings for the cluster.
 *
 * @since 3.0
 */
@Immutable
public final class ClusterSettings {
    private final List<ServerAddress> seedList;
    private final ClusterMode mode;
    private final String requiredReplicaSetName;

    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for the cluster settings.
     */
    public static final class Builder {
        private List<ServerAddress> seedList;
        private ClusterMode mode = ClusterMode.Discovering;
        private String requiredReplicaSetName;

        private Builder() {
        }

        // CHECKSTYLE:OFF

        /**
         * Sets the seed list for the cluster.
         *
         * @param seedList the seed list
         * @return this
         */
        public Builder seedList(final List<ServerAddress> seedList) {
            notNull("seedList", seedList);
            this.seedList = Collections.unmodifiableList(seedList);
            return this;
        }

        /**
         * Sets the mode for this cluster.
         *
         */
        public Builder mode(final ClusterMode mode) {
            notNull("mode", mode);
            this.mode = mode;
            return this;
        }

        /**
         * Sets the required replica set name for the cluster.
         *
         * @param requiredReplicaSetName the required replica set name.
         * @return this;
         */
        public Builder requiredReplicaSetName(final String requiredReplicaSetName) {
            this.requiredReplicaSetName = requiredReplicaSetName;
            return this;
        }

        /**
         * Build the settings from the builder.
         * @return the cluster settings
         */
        public ClusterSettings build() {
            return new ClusterSettings(this);
        }
        // CHECKSTYLE:ON
    }

    /**
     * Gets the seed list for the cluster.
     *
     * @return the seed list
     */
    public List<ServerAddress> getSeedList() {
        return seedList;
    }

    /**
     * Gets the mode.
     *
     * @return the mode
     */
    public ClusterMode getMode() {
        return mode;
    }

    /**
     * Gets the required replica set name.
     *
     * @return the required replica set name
     */
    public String getRequiredReplicaSetName() {
        return requiredReplicaSetName;
    }

    private ClusterSettings(final Builder builder) {
        notNull("seedList", builder.seedList);

        if (builder.mode == ClusterMode.Direct && builder.seedList.size() > 1) {
            throw new IllegalArgumentException("Can not directly connect to more than one server");
        }

        if (builder.mode == ClusterMode.Direct && builder.requiredReplicaSetName != null) {
            throw new IllegalArgumentException("Can not directly connect when there is a required replica set name");
        }

        seedList = Collections.unmodifiableList(builder.seedList);
        this.mode = builder.mode;
        requiredReplicaSetName = builder.requiredReplicaSetName;
    }
}
