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

import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * A server selector that composes a list of server selectors, and selects the servers by iterating through the list from start to
 * finish, passing the result of the previous into the next, and finally returning the result of the last one.
 *
 * @since 3.0
 */
public final class CompositeServerSelector implements ServerSelector {
    private final List<ServerSelector> serverSelectors;

    /**
     * Constructs a new instance.
     *
     * @param serverSelectors the list of composed server selectors
     */
    public CompositeServerSelector(final List<? extends ServerSelector> serverSelectors) {
        notNull("serverSelectors", serverSelectors);
        if (serverSelectors.isEmpty()) {
            throw new IllegalArgumentException("Server selectors can not be an empty list");
        }
        ArrayList<ServerSelector> mergedServerSelectors = new ArrayList<>();
        for (ServerSelector cur : serverSelectors) {
            if (cur == null) {
                throw new IllegalArgumentException("Can not have a null server selector in the list of composed selectors");
            }
            if (cur instanceof CompositeServerSelector) {
                mergedServerSelectors.addAll(((CompositeServerSelector) cur).serverSelectors);
            } else {
                mergedServerSelectors.add(cur);
            }
        }
        this.serverSelectors = Collections.unmodifiableList(mergedServerSelectors);
    }

    /**
     * @return the server selectors list.
     */
    public List<ServerSelector> getServerSelectors() {
        return serverSelectors;
    }

    @Override
    public List<ServerDescription> select(final ClusterDescription clusterDescription) {
        ClusterDescription curClusterDescription = clusterDescription;
        List<ServerDescription> choices = null;
        for (ServerSelector cur : serverSelectors) {
            choices = cur.select(curClusterDescription);
            curClusterDescription = new ClusterDescription(clusterDescription.getConnectionMode(), clusterDescription.getType(), choices,
                                                                  clusterDescription.getClusterSettings(),
                                                                  clusterDescription.getServerSettings());
        }

        return assertNotNull(choices);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CompositeServerSelector that = (CompositeServerSelector) o;
        if (serverSelectors.size() != that.serverSelectors.size()) {
            return false;
        }
        return serverSelectors.equals(that.serverSelectors);
    }

    @Override
    public int hashCode() {
        return serverSelectors != null ? serverSelectors.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "CompositeServerSelector{"
               + "serverSelectors=" + serverSelectors
               + '}';
    }
}
