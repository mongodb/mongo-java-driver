/**
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
 *
 */

package org.mongodb.rs;

import org.mongodb.annotations.Immutable;
import org.mongodb.MongoException;

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
    final List<ReplicaSetNode> all;
    final Random random;
    final List<ReplicaSetNode> goodSecondaries;
    final List<ReplicaSetNode> goodMembers;
    final ReplicaSetNode master;
    final String setName;
    final ReplicaSetErrorStatus errorStatus;

    private final int acceptableLatencyMS;

    public ReplicaSet(final List<ReplicaSetNode> nodeList, final Random random, final int acceptableLatencyMS) {

        this.random = random;
        this.all = Collections.unmodifiableList(new ArrayList<ReplicaSetNode>(nodeList));
        this.acceptableLatencyMS = acceptableLatencyMS;

        errorStatus = validate();
        setName = determineSetName();

        this.goodSecondaries =
                Collections.unmodifiableList(calculateGoodSecondaries(all, calculateBestPingTime(all), acceptableLatencyMS));
        this.goodMembers =
                Collections.unmodifiableList(calculateGoodMembers(all, calculateBestPingTime(all), acceptableLatencyMS));
        master = findMaster();
    }

    public List<ReplicaSetNode> getAll() {
        checkStatus();

        return all;
    }

    public boolean hasMaster() {
        return getMaster() != null;
    }

    public ReplicaSetNode getMaster() {
        checkStatus();

        return master;
    }

//    public int getMaxBsonObjectSize() {
//        if (hasMaster()) {
//            return getMaster().getMaxBsonObjectSize();
//        } else {
//            return Bytes.MAX_OBJECT_SIZE;
//        }
//    }

    public ReplicaSetNode getASecondary() {
        checkStatus();

        if (goodSecondaries.isEmpty()) {
            return null;
        }
        return goodSecondaries.get(random.nextInt(goodSecondaries.size()));
    }

    public ReplicaSetNode getASecondary(final List<Tag> tags) {
        checkStatus();

        // optimization
        if (tags.isEmpty()) {
            return getASecondary();
        }

        final List<ReplicaSetNode> acceptableTaggedSecondaries = getGoodSecondariesByTags(tags);

        if (acceptableTaggedSecondaries.isEmpty()) {
            return null;
        }
        return acceptableTaggedSecondaries.get(random.nextInt(acceptableTaggedSecondaries.size()));
    }

    public ReplicaSetNode getAMember() {
        checkStatus();

        if (goodMembers.isEmpty()) {
            return null;
        }
        return goodMembers.get(random.nextInt(goodMembers.size()));
    }

    public ReplicaSetNode getAMember(final List<Tag> tags) {
        checkStatus();

        if (tags.isEmpty()) {
            return getAMember();
        }

        final List<ReplicaSetNode> acceptableTaggedMembers = getGoodMembersByTags(tags);

        if (acceptableTaggedMembers.isEmpty()) {
            return null;
        }

        return acceptableTaggedMembers.get(random.nextInt(acceptableTaggedMembers.size()));
    }

    public List<ReplicaSetNode> getGoodSecondariesByTags(final List<Tag> tags) {
        checkStatus();

        final List<ReplicaSetNode> taggedSecondaries = getMembersByTags(all, tags);
        return calculateGoodSecondaries(taggedSecondaries,
                calculateBestPingTime(taggedSecondaries), acceptableLatencyMS);
    }

    public List<ReplicaSetNode> getGoodMembersByTags(final List<Tag> tags) {
        checkStatus();

        final List<ReplicaSetNode> taggedMembers = getMembersByTags(all, tags);
        return calculateGoodMembers(taggedMembers,
                calculateBestPingTime(taggedMembers), acceptableLatencyMS);
    }

    public List<ReplicaSetNode> getGoodMembers() {
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
        for (final ReplicaSetNode node : getAll()) {
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

    private ReplicaSetNode findMaster() {
        for (final ReplicaSetNode node : all) {
            if (node.master()) {
                return node;
            }
        }
        return null;
    }

    private String determineSetName() {
        for (final ReplicaSetNode node : all) {
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

        for (final ReplicaSetNode node : all) {
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

    static float calculateBestPingTime(final List<ReplicaSetNode> members) {
        float bestPingTime = Float.MAX_VALUE;
        for (final ReplicaSetNode cur : members) {
            if (!cur.secondary()) {
                continue;
            }
            if (cur._pingTime < bestPingTime) {
                bestPingTime = cur._pingTime;
            }
        }
        return bestPingTime;
    }

    static List<ReplicaSetNode> calculateGoodMembers(final List<ReplicaSetNode> members, final float bestPingTime,
                                                     final int acceptableLatencyMS) {
        final List<ReplicaSetNode> goodSecondaries = new ArrayList<ReplicaSetNode>(members.size());
        for (final ReplicaSetNode cur : members) {
            if (!cur.isOk()) {
                continue;
            }
            if (cur._pingTime - acceptableLatencyMS <= bestPingTime) {
                goodSecondaries.add(cur);
            }
        }
        return goodSecondaries;
    }

    static List<ReplicaSetNode> calculateGoodSecondaries(final List<ReplicaSetNode> members, final float bestPingTime,
                                                         final int acceptableLatencyMS) {
        final List<ReplicaSetNode> goodSecondaries = new ArrayList<ReplicaSetNode>(members.size());
        for (final ReplicaSetNode cur : members) {
            if (!cur.secondary()) {
                continue;
            }
            if (cur._pingTime - acceptableLatencyMS <= bestPingTime) {
                goodSecondaries.add(cur);
            }
        }
        return goodSecondaries;
    }

    static List<ReplicaSetNode> getMembersByTags(final List<ReplicaSetNode> members, final List<Tag> tags) {

        final List<ReplicaSetNode> membersByTag = new ArrayList<ReplicaSetNode>();

        for (final ReplicaSetNode cur : members) {
            if (tags != null && cur.getTags() != null && cur.getTags().containsAll(tags)) {
                membersByTag.add(cur);
            }
        }

        return membersByTag;
    }

}

