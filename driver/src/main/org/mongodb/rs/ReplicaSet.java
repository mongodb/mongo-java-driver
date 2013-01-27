/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package org.mongodb.rs;

import org.mongodb.MongoException;
import org.mongodb.ServerAddress;
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
public class ReplicaSet {
    private final List<ReplicaSetMember> all;
    private final Random random;
    private final List<ReplicaSetMember> goodSecondaries;
    private final List<ReplicaSetMember> goodMembers;
    private final ReplicaSetMember primary;
    private final String setName;
    private final ReplicaSetErrorStatus errorStatus;

    private final int acceptableLatencyMS;

    public ReplicaSet(final List<ReplicaSetMember> nodeList, final Random random, final int acceptableLatencyMS) {

        this.random = random;
        this.all = Collections.unmodifiableList(new ArrayList<ReplicaSetMember>(nodeList));
        this.acceptableLatencyMS = acceptableLatencyMS;

        errorStatus = validate();
        setName = determineSetName();

        this.goodSecondaries =
        Collections.unmodifiableList(calculateGoodSecondaries(all, calculateBestPingTime(all), acceptableLatencyMS));
        this.goodMembers =
        Collections.unmodifiableList(calculateGoodMembers(all, calculateBestPingTime(all), acceptableLatencyMS));
        primary = findPrimary();
    }

    public List<ReplicaSetMember> getAll() {
        checkStatus();

        return all;
    }

    public ReplicaSetMember getMember(final ServerAddress serverAddress) {
        checkStatus();
        for (ReplicaSetMember cur : all) {
            if (cur.getServerAddress().equals(serverAddress)) {
                return cur;
            }
        }
        return null;
    }

    public boolean hasPrimary() {
        return getPrimary() != null;
    }

    public ReplicaSetMember getPrimary() {
        checkStatus();

        return primary;
    }

    //    public int getMaxBsonObjectSize() {
    //        if (hasMaster()) {
    //            return getMaster().getMaxBsonObjectSize();
    //        } else {
    //            return Bytes.MAX_OBJECT_SIZE;
    //        }
    //    }

    public ReplicaSetMember getASecondary() {
        checkStatus();

        if (goodSecondaries.isEmpty()) {
            return null;
        }
        return goodSecondaries.get(random.nextInt(goodSecondaries.size()));
    }

    public ReplicaSetMember getASecondary(final List<Tag> tags) {
        checkStatus();

        // optimization
        if (tags.isEmpty()) {
            return getASecondary();
        }

        final List<ReplicaSetMember> acceptableTaggedSecondaries = getGoodSecondariesByTags(tags);

        if (acceptableTaggedSecondaries.isEmpty()) {
            return null;
        }
        return acceptableTaggedSecondaries.get(random.nextInt(acceptableTaggedSecondaries.size()));
    }

    public ReplicaSetMember getAMember() {
        checkStatus();

        if (goodMembers.isEmpty()) {
            return null;
        }
        return goodMembers.get(random.nextInt(goodMembers.size()));
    }

    public ReplicaSetMember getAMember(final List<Tag> tags) {
        checkStatus();

        if (tags.isEmpty()) {
            return getAMember();
        }

        final List<ReplicaSetMember> acceptableTaggedMembers = getGoodMembersByTags(tags);

        if (acceptableTaggedMembers.isEmpty()) {
            return null;
        }

        return acceptableTaggedMembers.get(random.nextInt(acceptableTaggedMembers.size()));
    }

    public List<ReplicaSetMember> getGoodSecondariesByTags(final List<Tag> tags) {
        checkStatus();

        final List<ReplicaSetMember> taggedSecondaries = getMembersByTags(all, tags);
        return calculateGoodSecondaries(taggedSecondaries,
                                       calculateBestPingTime(taggedSecondaries), acceptableLatencyMS);
    }

    public List<ReplicaSetMember> getGoodMembersByTags(final List<Tag> tags) {
        checkStatus();

        final List<ReplicaSetMember> taggedMembers = getMembersByTags(all, tags);
        return calculateGoodMembers(taggedMembers,
                                   calculateBestPingTime(taggedMembers), acceptableLatencyMS);
    }

    public List<ReplicaSetMember> getGoodMembers() {
        checkStatus();

        return calculateGoodMembers(all, calculateBestPingTime(all), acceptableLatencyMS);
    }

    public String getSetName() {
        checkStatus();

        return setName;
    }

    public ReplicaSetErrorStatus getErrorStatus() {
        return errorStatus;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[ ");
        for (final ReplicaSetMember node : getAll()) {
            sb.append(node.toJSON()).append(",");
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

    private ReplicaSetMember findPrimary() {
        for (final ReplicaSetMember node : all) {
            if (node.primary()) {
                return node;
            }
        }
        return null;
    }

    private String determineSetName() {
        for (final ReplicaSetMember node : all) {
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

        for (final ReplicaSetMember node : all) {
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

    static float calculateBestPingTime(final List<ReplicaSetMember> members) {
        float bestPingTime = Float.MAX_VALUE;
        for (final ReplicaSetMember cur : members) {
            if (!cur.secondary()) {
                continue;
            }
            if (cur.getPingTime() < bestPingTime) {
                bestPingTime = cur.getPingTime();
            }
        }
        return bestPingTime;
    }

    static List<ReplicaSetMember> calculateGoodMembers(final List<ReplicaSetMember> members, final float bestPingTime,
                                                     final int acceptableLatencyMS) {
        final List<ReplicaSetMember> goodSecondaries = new ArrayList<ReplicaSetMember>(members.size());
        for (final ReplicaSetMember cur : members) {
            if (!cur.isOk()) {
                continue;
            }
            if (cur.getPingTime() - acceptableLatencyMS <= bestPingTime) {
                goodSecondaries.add(cur);
            }
        }
        return goodSecondaries;
    }

    static List<ReplicaSetMember> calculateGoodSecondaries(final List<ReplicaSetMember> members, final float bestPingTime,
                                                         final int acceptableLatencyMS) {
        final List<ReplicaSetMember> goodSecondaries = new ArrayList<ReplicaSetMember>(members.size());
        for (final ReplicaSetMember cur : members) {
            if (!cur.secondary()) {
                continue;
            }
            if (cur.getPingTime() - acceptableLatencyMS <= bestPingTime) {
                goodSecondaries.add(cur);
            }
        }
        return goodSecondaries;
    }

    static List<ReplicaSetMember> getMembersByTags(final List<ReplicaSetMember> members, final List<Tag> tags) {

        final List<ReplicaSetMember> membersByTag = new ArrayList<ReplicaSetMember>();

        for (final ReplicaSetMember cur : members) {
            if (tags != null && cur.getTags() != null && cur.getTags().containsAll(tags)) {
                membersByTag.add(cur);
            }
        }

        return membersByTag;
    }

}

