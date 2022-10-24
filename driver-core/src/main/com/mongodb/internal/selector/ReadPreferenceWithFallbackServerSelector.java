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

package com.mongodb.internal.selector;

import com.mongodb.ReadPreference;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.selector.ServerSelector;

import java.util.List;

public class ReadPreferenceWithFallbackServerSelector implements ServerSelector {

    private final ReadPreference preferredReadPreference;
    private final int minWireVersion;
    private final ReadPreference fallbackReadPreference;
    private ReadPreference appliedReadPreference;

    public ReadPreferenceWithFallbackServerSelector(final ReadPreference preferredReadPreference, final int minWireVersion,
            final ReadPreference fallbackReadPreference) {
        this.preferredReadPreference = preferredReadPreference;
        this.minWireVersion = minWireVersion;
        this.fallbackReadPreference = fallbackReadPreference;
    }


    @Override
    public List<ServerDescription> select(final ClusterDescription clusterDescription) {
        if (clusterContainsOlderServers(clusterDescription)) {
            appliedReadPreference = fallbackReadPreference;
            return new ReadPreferenceServerSelector(fallbackReadPreference).select(clusterDescription);
        } else {
            appliedReadPreference = preferredReadPreference;
            return new ReadPreferenceServerSelector(preferredReadPreference).select(clusterDescription);
        }
    }

    public ReadPreference getAppliedReadPreference() {
        return appliedReadPreference;
    }

    private boolean clusterContainsOlderServers(final ClusterDescription clusterDescription) {
        return clusterDescription.getServerDescriptions().stream()
                .filter(serverDescription -> serverDescription.getState() == ServerConnectionState.CONNECTED)
                .anyMatch(serverDescription -> serverDescription.getMaxWireVersion() < minWireVersion);
    }

    @Override
    public String toString() {
        return "ReadPreferenceWithFallbackServerSelector{"
                + "preferredReadPreference=" + preferredReadPreference
                + ", fallbackReadPreference=" + fallbackReadPreference
                + ", minWireVersionForPreferred=" + minWireVersion
                + '}';
    }
}
