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

import com.mongodb.annotations.Immutable;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Abstract class for all preference which can be combined with tags
 */
@Immutable
public abstract class TaggableReadPreference extends ReadPreference {
    private static final int SMALLEST_MAX_STALENESS_MS = 90000;
    private static final int IDLE_WRITE_PERIOD_MS = 10000;

    private final List<TagSet> tagSetList = new ArrayList<TagSet>();
    private final Long maxStalenessMS;

    TaggableReadPreference() {
        this.maxStalenessMS = null;
    }

    TaggableReadPreference(final List<TagSet> tagSetList, final Long maxStaleness, final TimeUnit timeUnit) {
        notNull("tagSetList", tagSetList);
        isTrueArgument("maxStaleness is null or >= 0", maxStaleness == null || maxStaleness >= 0);
        this.maxStalenessMS = maxStaleness == null ? null : MILLISECONDS.convert(maxStaleness, timeUnit);

        for (final TagSet tagSet : tagSetList) {
            this.tagSetList.add(tagSet);
        }
    }

    @Override
    public boolean isSlaveOk() {
        return true;
    }

    @Override
    public BsonDocument toDocument() {
        BsonDocument readPrefObject = new BsonDocument("mode", new BsonString(getName()));

        if (!tagSetList.isEmpty()) {
            readPrefObject.put("tags", tagsListToBsonArray());
        }

        if (maxStalenessMS != null) {
            readPrefObject.put("maxStalenessSeconds", new BsonInt64(MILLISECONDS.toSeconds(maxStalenessMS)));
        }
        return readPrefObject;
    }

    /**
     * Gets the list of tag sets as a list of {@code TagSet} instances.
     *
     * @return the list of tag sets
     * @since 2.13
     */
    public List<TagSet> getTagSetList() {
        return Collections.unmodifiableList(tagSetList);
    }

    /**
     * Gets the maximum acceptable staleness of a secondary in order to be considered for read operations.
     * <p>
     * The maximum staleness feature is designed to prevent badly-lagging servers from being selected. The staleness estimate is imprecise
     * and shouldn't be used to try to select "up-to-date" secondaries.
     * </p>
     * <p>
     * The driver estimates the staleness of each secondary, based on lastWriteDate values provided in server isMaster responses,
     * and selects only those secondaries whose staleness is less than or equal to maxStaleness.
     * </p>
     * @param timeUnit the time unit in which to return the value
     * @return the maximum acceptable staleness in the given time unit, or null if the value is not set
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public Long getMaxStaleness(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        if (maxStalenessMS == null) {
            return null;
        }
        return timeUnit.convert(maxStalenessMS, TimeUnit.MILLISECONDS);
    }

    @Override
    public String toString() {
        return "ReadPreference{"
                       + "name=" + getName()
                       + (tagSetList.isEmpty() ? "" : ", tagSetList=" + tagSetList)
                       + (maxStalenessMS == null ? "" : ", maxStalenessMS=" + maxStalenessMS)
                       + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TaggableReadPreference that = (TaggableReadPreference) o;

        if (maxStalenessMS != null ? !maxStalenessMS.equals(that.maxStalenessMS) : that.maxStalenessMS != null) {
            return false;
        }
        if (!tagSetList.equals(that.tagSetList)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = tagSetList.hashCode();
        result = 31 * result + getName().hashCode();
        result = 31 * result + (maxStalenessMS != null ? maxStalenessMS.hashCode() : 0);
        return result;
    }

    @Override
    @SuppressWarnings("deprecation")
    protected List<ServerDescription> chooseForNonReplicaSet(final ClusterDescription clusterDescription) {
        return selectFreshServers(clusterDescription, clusterDescription.getAny());
    }

    protected static ClusterDescription copyClusterDescription(final ClusterDescription clusterDescription,
                                                               final List<ServerDescription> selectedServers) {
        return new ClusterDescription(clusterDescription.getConnectionMode(),
                                             clusterDescription.getType(),
                                             selectedServers,
                                             clusterDescription.getClusterSettings(),
                                             clusterDescription.getServerSettings());
    }

    protected List<ServerDescription> selectFreshServers(final ClusterDescription clusterDescription,
                                                         final List<ServerDescription> servers) {
        if (getMaxStaleness(MILLISECONDS) == null) {
            return servers;
        }

        if (clusterDescription.getServerSettings() == null) {
            throw new MongoConfigurationException("heartbeat frequency must be provided in cluster description");
        }

        if (!serversAreAllThreeDotFour(clusterDescription)) {
            throw new MongoConfigurationException("Servers must all be at least version 3.4 when max staleness is configured");
        }

        if (clusterDescription.getType() != ClusterType.REPLICA_SET) {
            return servers;
        }

        long heartbeatFrequencyMS = clusterDescription.getServerSettings().getHeartbeatFrequency(MILLISECONDS);

        if (getMaxStaleness(MILLISECONDS) < Math.max(SMALLEST_MAX_STALENESS_MS, heartbeatFrequencyMS + IDLE_WRITE_PERIOD_MS)) {
            if (SMALLEST_MAX_STALENESS_MS > heartbeatFrequencyMS + IDLE_WRITE_PERIOD_MS){
                throw new MongoConfigurationException(format("Max staleness (%d sec) must be at least 90 seconds",
                        getMaxStaleness(SECONDS)));
            } else {
                throw new MongoConfigurationException(format("Max staleness (%d ms) must be at least the heartbeat period (%d ms) "
                                                                     + "plus the idle write period (%d ms)",
                        getMaxStaleness(MILLISECONDS), heartbeatFrequencyMS, IDLE_WRITE_PERIOD_MS));
            }
        }
        List<ServerDescription> freshServers = new ArrayList<ServerDescription>(servers.size());

        ServerDescription primary = findPrimary(clusterDescription);

        if (primary != null) {
            for (ServerDescription cur : servers) {
                if (cur.isPrimary()) {
                    freshServers.add(cur);
                } else {
                    if (getStalenessOfSecondaryRelativeToPrimary(primary, cur, heartbeatFrequencyMS) <= getMaxStaleness(MILLISECONDS)) {
                        freshServers.add(cur);
                    }
                }
            }
        } else {
            ServerDescription mostUpdateToDateSecondary = findMostUpToDateSecondary(clusterDescription);
            for (ServerDescription cur : servers) {
                if (mostUpdateToDateSecondary.getLastWriteDate().getTime() - cur.getLastWriteDate().getTime() + heartbeatFrequencyMS
                            <= getMaxStaleness(MILLISECONDS)) {
                    freshServers.add(cur);
                }
            }
        }

        return freshServers;
    }

    private long getStalenessOfSecondaryRelativeToPrimary(final ServerDescription primary, final ServerDescription serverDescription,
                                                          final long heartbeatFrequencyMS) {
        return primary.getLastWriteDate().getTime()
                       + (serverDescription.getLastUpdateTime(MILLISECONDS) - primary.getLastUpdateTime(MILLISECONDS))
                       - serverDescription.getLastWriteDate().getTime() + heartbeatFrequencyMS;
    }

    private ServerDescription findPrimary(final ClusterDescription clusterDescription) {
        for (ServerDescription cur : clusterDescription.getServerDescriptions()) {
            if (cur.isPrimary()) {
                return cur;
            }
        }
        return null;
    }

    private ServerDescription findMostUpToDateSecondary(final ClusterDescription clusterDescription) {
        ServerDescription mostUpdateToDateSecondary = null;
        for (ServerDescription cur : clusterDescription.getServerDescriptions()) {
            if (cur.isSecondary()) {
                if (mostUpdateToDateSecondary == null
                            || cur.getLastWriteDate().getTime() > mostUpdateToDateSecondary.getLastWriteDate().getTime()) {
                    mostUpdateToDateSecondary = cur;
                }
            }
        }
        return mostUpdateToDateSecondary;
    }

    private boolean serversAreAllThreeDotFour(final ClusterDescription clusterDescription) {
        for (ServerDescription cur : clusterDescription.getServerDescriptions()) {
            if (cur.isOk() && cur.getMaxWireVersion() < 5) {
                return false;
            }
        }
        return true;
    }

    /**
     * Read from secondary
     */
    static class SecondaryReadPreference extends TaggableReadPreference {
        SecondaryReadPreference() {
        }

        SecondaryReadPreference(final List<TagSet> tagSetList, final Long maxStaleness, final TimeUnit timeUnit) {
            super(tagSetList, maxStaleness, timeUnit);
        }

        @Override
        public String getName() {
            return "secondary";
        }

        @Override
        @SuppressWarnings("deprecation")
        protected List<ServerDescription> chooseForReplicaSet(final ClusterDescription clusterDescription) {
            List<ServerDescription> selectedServers = selectFreshServers(clusterDescription, clusterDescription.getSecondaries());
            if (!getTagSetList().isEmpty()) {
                ClusterDescription nonStaleClusterDescription = copyClusterDescription(clusterDescription, selectedServers);
                selectedServers = Collections.emptyList();
                for (final TagSet tagSet : getTagSetList()) {
                    List<ServerDescription> servers = nonStaleClusterDescription.getSecondaries(tagSet);
                    if (!servers.isEmpty()) {
                        selectedServers = servers;
                        break;
                    }
                }
            }
            return selectedServers;
        }
    }

    /**
     * Read from secondary if available, otherwise from primary, irrespective of tags.
     */
    static class SecondaryPreferredReadPreference extends SecondaryReadPreference {
        SecondaryPreferredReadPreference() {
        }

        SecondaryPreferredReadPreference(final List<TagSet> tagSetList, final Long maxStaleness, final TimeUnit timeUnit) {
            super(tagSetList, maxStaleness, timeUnit);
        }

        @Override
        public String getName() {
            return "secondaryPreferred";
        }

        @Override
        @SuppressWarnings("deprecation")
        protected List<ServerDescription> chooseForReplicaSet(final ClusterDescription clusterDescription) {
            List<ServerDescription> selectedServers = super.chooseForReplicaSet(clusterDescription);
            if (selectedServers.isEmpty()) {
                selectedServers = clusterDescription.getPrimaries();
            }
            return selectedServers;
        }
    }

    /**
     * Read from nearest node respective of tags.
     */
    static class NearestReadPreference extends TaggableReadPreference {
        NearestReadPreference() {
        }

        NearestReadPreference(final List<TagSet> tagSetList, final Long maxStaleness, final TimeUnit timeUnit) {
            super(tagSetList, maxStaleness, timeUnit);
        }


        @Override
        public String getName() {
            return "nearest";
        }


        @Override
        @SuppressWarnings("deprecation")
        public List<ServerDescription> chooseForReplicaSet(final ClusterDescription clusterDescription) {
            List<ServerDescription> selectedServers = selectFreshServers(clusterDescription, clusterDescription.getAny());
            if (!getTagSetList().isEmpty()) {
                ClusterDescription nonStaleClusterDescription = copyClusterDescription(clusterDescription, selectedServers);
                selectedServers = Collections.emptyList();
                for (final TagSet tagSet : getTagSetList()) {
                    List<ServerDescription> servers = nonStaleClusterDescription.getAnyPrimaryOrSecondary(tagSet);
                    if (!servers.isEmpty()) {
                        selectedServers = servers;
                        break;
                    }
                }
            }
            return selectedServers;
        }
    }

    /**
     * Read from primary if available, otherwise a secondary.
     */
    static class PrimaryPreferredReadPreference extends SecondaryReadPreference {
        PrimaryPreferredReadPreference() {
        }

        PrimaryPreferredReadPreference(final List<TagSet> tagSetList, final Long maxStaleness, final TimeUnit timeUnit) {
            super(tagSetList, maxStaleness, timeUnit);
        }

        @Override
        public String getName() {
            return "primaryPreferred";
        }

        @Override
        @SuppressWarnings("deprecation")
        protected List<ServerDescription> chooseForReplicaSet(final ClusterDescription clusterDescription) {
            List<ServerDescription> selectedServers = selectFreshServers(clusterDescription, clusterDescription.getPrimaries());
            if (selectedServers.isEmpty()) {
                selectedServers = super.chooseForReplicaSet(clusterDescription);
            }
            return selectedServers;
        }
    }

    private BsonArray tagsListToBsonArray() {
        BsonArray bsonArray = new BsonArray();
        for (TagSet tagSet : tagSetList) {
            bsonArray.add(toDocument(tagSet));
        }
        return bsonArray;
    }

    private BsonDocument toDocument(final TagSet tagSet) {
        BsonDocument document = new BsonDocument();

        for (Tag tag : tagSet) {
            document.put(tag.getName(), new BsonString(tag.getValue()));
        }

        return document;
    }

}
