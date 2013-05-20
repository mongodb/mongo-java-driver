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
    private final List<ServerDescription> goodPrimaries;
    private final List<ServerDescription> goodSecondaries;
    private final List<ServerDescription> goodMembers;

    private final int acceptableLatencyMS;

    public ClusterDescription(final List<ServerDescription> serverDescriptions, final int acceptableLatencyMS) {

        this.all = Collections.unmodifiableList(new ArrayList<ServerDescription>(serverDescriptions));
        this.acceptableLatencyMS = acceptableLatencyMS;

        this.goodPrimaries = Collections.unmodifiableList(calculateGoodPrimaries(all, calculateBestPingTime(all), acceptableLatencyMS));
        this.goodSecondaries = Collections.unmodifiableList(calculateGoodSecondaries(all, calculateBestPingTime(all), acceptableLatencyMS));
        this.goodMembers = Collections.unmodifiableList(calculateGoodMembers(all, calculateBestPingTime(all), acceptableLatencyMS));
    }

    public List<ServerDescription> getAll() {
        return all;
    }

    /**
     * While it may seem counter-intuitive that a MongoDb cluster can have more than one primary,
     * it can in the case where the client's view of the cluster is a set of mongos servers, any of which can serve as the primary.
     *
     * @return a list of servers that can act as primaries\
     */
    public List<ServerDescription> getPrimaries() {
        return goodPrimaries;
    }

    public List<ServerDescription> getSecondaries() {
        return goodSecondaries;
    }

    public List<ServerDescription> getSecondaries(final Tags tags) {
        List<ServerDescription> taggedSecondaries = getMembersByTags(all, tags);
        return calculateGoodSecondaries(taggedSecondaries, calculateBestPingTime(taggedSecondaries), acceptableLatencyMS);
    }

    public List<ServerDescription> getAny() {
        return goodMembers;
    }

    public List<ServerDescription> getAny(final Tags tags) {
        List<ServerDescription> taggedMembers = getMembersByTags(all, tags);
        return calculateGoodMembers(taggedMembers, calculateBestPingTime(taggedMembers), acceptableLatencyMS);
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

    static List<ServerDescription> calculateGoodPrimaries(final List<ServerDescription> members,
                                                            final float bestPingTime, final int acceptableLatencyMS) {
        final List<ServerDescription> goodPrimaries = new ArrayList<ServerDescription>(members.size());
        for (final ServerDescription cur : members) {
            if (!cur.isPrimary()) {
                continue;
            }
            if (cur.getAveragePingTimeMillis() - acceptableLatencyMS <= bestPingTime) {
                goodPrimaries.add(cur);
            }
        }
        return goodPrimaries;
    }

    static List<ServerDescription> calculateGoodMembers(final List<ServerDescription> members, final float bestPingTime,
                                                        final int acceptableLatencyMS) {
        final List<ServerDescription> goodSecondaries = new ArrayList<ServerDescription>(members.size());
        for (final ServerDescription cur : members) {
            if (!cur.isOk()) {
                continue;
            }
            if (cur.getAveragePingTimeMillis() - acceptableLatencyMS <= bestPingTime) {
                goodSecondaries.add(cur);
            }
        }
        return goodSecondaries;
    }

    static List<ServerDescription> calculateGoodSecondaries(final List<ServerDescription> members,
                                                            final float bestPingTime, final int acceptableLatencyMS) {
        final List<ServerDescription> goodSecondaries = new ArrayList<ServerDescription>(members.size());
        for (final ServerDescription cur : members) {
            if (!cur.isSecondary()) {
                continue;
            }
            if (cur.getAveragePingTimeMillis() - acceptableLatencyMS <= bestPingTime) {
                goodSecondaries.add(cur);
            }
        }
        return goodSecondaries;
    }

    static List<ServerDescription> getMembersByTags(final List<ServerDescription> members, final Tags tags) {

        final List<ServerDescription> membersByTag = new ArrayList<ServerDescription>();

        for (final ServerDescription cur : members) {
            if (cur.hasTags(tags)) {
                membersByTag.add(cur);
            }
        }

        return membersByTag;
    }
}

