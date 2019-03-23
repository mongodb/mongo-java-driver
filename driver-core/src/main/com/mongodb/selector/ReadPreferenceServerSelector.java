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

package com.mongodb.selector;

import com.mongodb.ReadPreference;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getAny;

/**
 * A server selector that chooses based on a read preference.
 *
 * @since 3.0
 */
@Deprecated
public class ReadPreferenceServerSelector implements ServerSelector {
    private final ReadPreference readPreference;

    /**
     * Gets the read preference.
     *
     * @param readPreference the read preference
     */
    public ReadPreferenceServerSelector(final ReadPreference readPreference) {
        this.readPreference = notNull("readPreference", readPreference);
    }

    /**
     * Gets the read preference.
     *
     * @return the read preference
     */
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    @SuppressWarnings("deprecation")
    public List<ServerDescription> select(final ClusterDescription clusterDescription) {
        if (clusterDescription.getConnectionMode() == ClusterConnectionMode.SINGLE) {
            return getAny(clusterDescription);
        }
        return readPreference.choose(clusterDescription);
    }

    @Override
    public String toString() {
        return "ReadPreferenceServerSelector{"
               + "readPreference=" + readPreference
               + '}';
    }
}
