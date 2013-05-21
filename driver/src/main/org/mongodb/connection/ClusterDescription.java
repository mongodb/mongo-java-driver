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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Immutable snapshot state of a cluster.
 */
@Immutable
public class ClusterDescription {
    private final List<ServerDescription> all;

    private final int acceptableLatencyMS;

    public ClusterDescription(final List<ServerDescription> all, final int acceptableLatencyMS) {
        this.all = Collections.unmodifiableList(new ArrayList<ServerDescription>(all));
        this.acceptableLatencyMS = acceptableLatencyMS;
    }

    public List<ServerDescription> getAll() {
        return all;
    }


    public ServerDescription getByServerAddress(final ServerAddress serverAddress) {
        for (ServerDescription cur : getAll()) {
            if (cur.getAddress().equals(serverAddress)) {
                return cur;
            }
        }
        return null;
    }

    /**
     * While it may seem counter-intuitive that a MongoDb cluster can have more than one primary,
     * it can in the case where the client's view of the cluster is a set of mongos servers, any of which can serve as the primary.
     *
     * @return a list of servers that can act as primaries\
     */
    public List<ServerDescription> getPrimaries() {
        List<ServerDescription> primaries = getAllGoodPrimaries(all);
        return getAllServersWithAcceptableLatency(primaries, calculateBestPingTime(primaries), acceptableLatencyMS);
    }

    public List<ServerDescription> getSecondaries() {
        List<ServerDescription> secondaries = getAllGoodSecondaries(all);
        return getAllServersWithAcceptableLatency(secondaries, calculateBestPingTime(secondaries), acceptableLatencyMS);
    }

    public List<ServerDescription> getSecondaries(final Tags tags) {
        List<ServerDescription> taggedServers = getServersByTags(all, tags);
        List<ServerDescription> taggedSecondaries = getAllGoodSecondaries(taggedServers);
        return getAllServersWithAcceptableLatency(taggedSecondaries, calculateBestPingTime(taggedSecondaries), acceptableLatencyMS);
    }

    public List<ServerDescription> getAny() {
        List<ServerDescription> any = getAllGoodServers(all);
        return getAllServersWithAcceptableLatency(any, calculateBestPingTime(any), acceptableLatencyMS);
    }

    public List<ServerDescription> getAny(final Tags tags) {
        List<ServerDescription> taggedServers = getServersByTags(all, tags);
        List<ServerDescription> taggedAny = getAllGoodServers(taggedServers);
        return getAllServersWithAcceptableLatency(taggedAny, calculateBestPingTime(taggedAny), acceptableLatencyMS);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[ ");
        for (final ServerDescription node : getAll()) {
            sb.append(node).append(",");
        }
        sb.setLength(sb.length() - 1); //remove last comma
        sb.append(" ]");
        return sb.toString();
    }

    static float calculateBestPingTime(final List<ServerDescription> members) {
        float bestPingTime = Float.MAX_VALUE;
        for (final ServerDescription cur : members) {
            if (!cur.isSecondary()) {
                continue;
            }
            if (cur.getAveragePingTimeMillis() < bestPingTime) {
                bestPingTime = cur.getAveragePingTimeMillis();
            }
        }
        return bestPingTime;
    }

    static List<ServerDescription> getAllGoodPrimaries(final List<ServerDescription> servers) {
        final List<ServerDescription> goodPrimaries = new ArrayList<ServerDescription>(servers.size());
        for (final ServerDescription cur : servers) {
            if (cur.isPrimary()) {
                goodPrimaries.add(cur);
            }
        }
        return goodPrimaries;
    }

    static List<ServerDescription> getAllGoodServers(final List<ServerDescription> servers) {
        final List<ServerDescription> goodSecondaries = new ArrayList<ServerDescription>(servers.size());
        for (final ServerDescription cur : servers) {
            if (cur.isOk()) {
                goodSecondaries.add(cur);
            }
        }
        return goodSecondaries;
    }

    static List<ServerDescription> getAllGoodSecondaries(final List<ServerDescription> server) {
        final List<ServerDescription> goodSecondaries = new ArrayList<ServerDescription>(server.size());
        for (final ServerDescription cur : server) {
            if (cur.isSecondary()) {
                goodSecondaries.add(cur);
            }
        }
        return goodSecondaries;
    }

    static List<ServerDescription> getAllServersWithAcceptableLatency(final List<ServerDescription> servers,
                                                                      final float bestPingTime, final int acceptableLatencyMS) {
        final List<ServerDescription> goodSecondaries = new ArrayList<ServerDescription>(servers.size());
        for (final ServerDescription cur : servers) {
            if (cur.getAveragePingTimeMillis() - acceptableLatencyMS <= bestPingTime) {
                goodSecondaries.add(cur);
            }
        }
        return goodSecondaries;
    }

    static List<ServerDescription> getServersByTags(final List<ServerDescription> servers, final Tags tags) {

        final List<ServerDescription> membersByTag = new ArrayList<ServerDescription>();

        for (final ServerDescription cur : servers) {
            if (cur.hasTags(tags)) {
                membersByTag.add(cur);
            }
        }

        return membersByTag;
    }
}

