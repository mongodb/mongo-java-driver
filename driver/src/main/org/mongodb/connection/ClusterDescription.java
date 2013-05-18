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

import org.mongodb.MongoException;
import org.mongodb.annotations.Immutable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;


/**
 * Immutable snapshot state of a replica set. Since the nodes don't change state, this class pre-computes the list of
 * good secondaries so that choosing a random good secondary is dead simple.
 * <p/>
 * NOT PART OF PUBLIC API YET
 */
@Immutable
public class ClusterDescription {
    private final List<ServerDescription> all;
    private final Random random;
    private final List<ServerDescription> goodSecondaries;
    private final List<ServerDescription> goodMembers;
    private final ServerDescription primary;
    private final String setName;
    private final ReplicaSetErrorStatus errorStatus;

    private final int acceptableLatencyMS;

    public ClusterDescription(final List<ServerDescription> nodeList, final Random random, final int acceptableLatencyMS) {

        this.random = random;
        this.all = Collections.unmodifiableList(new ArrayList<ServerDescription>(nodeList));
        this.acceptableLatencyMS = acceptableLatencyMS;

        errorStatus = validate();
        setName = determineSetName();

        this.goodSecondaries =
        Collections.unmodifiableList(calculateGoodSecondaries(all, calculateBestPingTime(all), acceptableLatencyMS));
        this.goodMembers =
        Collections.unmodifiableList(calculateGoodMembers(all, calculateBestPingTime(all), acceptableLatencyMS));
        primary = findPrimary();
    }

    public List<ServerDescription> getAll() {
        checkStatus();

        return all;
    }

    public ServerDescription getPrimary() {
        checkStatus();

        return primary;
    }

    public ServerDescription getASecondary() {
        checkStatus();

        if (goodSecondaries.isEmpty()) {
            return null;
        }
        return goodSecondaries.get(random.nextInt(goodSecondaries.size()));
    }

    public ServerDescription getASecondary(final List<Tag> tags) {
        checkStatus();

        // optimization
        if (tags.isEmpty()) {
            return getASecondary();
        }

        final List<ServerDescription> acceptableTaggedSecondaries = getGoodSecondariesByTags(tags);

        if (acceptableTaggedSecondaries.isEmpty()) {
            return null;
        }
        return acceptableTaggedSecondaries.get(random.nextInt(acceptableTaggedSecondaries.size()));
    }

    public ServerDescription getAMember() {
        checkStatus();

        if (goodMembers.isEmpty()) {
            return null;
        }
        return goodMembers.get(random.nextInt(goodMembers.size()));
    }

    public ServerDescription getAMember(final List<Tag> tags) {
        checkStatus();

        if (tags.isEmpty()) {
            return getAMember();
        }

        final List<ServerDescription> acceptableTaggedMembers = getGoodMembersByTags(tags);

        if (acceptableTaggedMembers.isEmpty()) {
            return null;
        }

        return acceptableTaggedMembers.get(random.nextInt(acceptableTaggedMembers.size()));
    }

    public List<ServerDescription> getGoodSecondariesByTags(final List<Tag> tags) {
        checkStatus();

        final List<ServerDescription> taggedSecondaries = getMembersByTags(all, tags);
        return calculateGoodSecondaries(taggedSecondaries,
                                       calculateBestPingTime(taggedSecondaries), acceptableLatencyMS);
    }

    public List<ServerDescription> getGoodMembersByTags(final List<Tag> tags) {
        checkStatus();

        final List<ServerDescription> taggedMembers = getMembersByTags(all, tags);
        return calculateGoodMembers(taggedMembers,
                                   calculateBestPingTime(taggedMembers), acceptableLatencyMS);
    }

    public String getSetName() {
        checkStatus();

        return setName;
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

    private void checkStatus() {
        if (!errorStatus.isOk()) {
            throw new MongoException(errorStatus.getError());
        }
    }

    private ServerDescription findPrimary() {
        for (final ServerDescription node : all) {
            if (node.isPrimary()) {
                return node;
            }
        }
        return null;
    }

    private String determineSetName() {
        for (final ServerDescription node : all) {
            final String nodeSetName = node.getSetName();

            if (nodeSetName != null && !nodeSetName.equals("")) {
                return nodeSetName;
            }
        }

        return null;
    }

    private ReplicaSetErrorStatus validate() {
        //make sure all nodes have the same set name
        final HashSet<String> nodeNames = new HashSet<String>();

        for (final ServerDescription node : all) {
            final String nodeSetName = node.getSetName();

            if (nodeSetName != null && !nodeSetName.equals("")) {
                nodeNames.add(nodeSetName);
            }
        }

        if (nodeNames.size() <= 1) {
            return new ReplicaSetErrorStatus(true, null);
        }
        else {
            return new ReplicaSetErrorStatus(false, "nodes with different set names detected: " + nodeNames.toString());
        }
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

    static List<ServerDescription> getMembersByTags(final List<ServerDescription> members, final List<Tag> tags) {

        final List<ServerDescription> membersByTag = new ArrayList<ServerDescription>();

        for (final ServerDescription cur : members) {
            if (tags != null && cur.getTags() != null && cur.getTags().containsAll(tags)) {
                membersByTag.add(cur);
            }
        }

        return membersByTag;
    }

}

