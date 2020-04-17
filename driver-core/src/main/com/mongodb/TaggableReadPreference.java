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

package com.mongodb;

import com.mongodb.annotations.Immutable;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getAny;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getAnyPrimaryOrSecondary;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getPrimaries;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getSecondaries;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
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
    private final ReadPreferenceHedgeOptions hedgeOptions;

    TaggableReadPreference() {
        this.maxStalenessMS = null;
        this.hedgeOptions = null;
    }

    TaggableReadPreference(final List<TagSet> tagSetList, @Nullable final Long maxStaleness, final TimeUnit timeUnit,
                           @Nullable final ReadPreferenceHedgeOptions hedgeOptions) {
        notNull("tagSetList", tagSetList);
        isTrueArgument("maxStaleness is null or >= 0", maxStaleness == null || maxStaleness >= 0);
        this.maxStalenessMS = maxStaleness == null ? null : MILLISECONDS.convert(maxStaleness, timeUnit);

        this.tagSetList.addAll(tagSetList);
        this.hedgeOptions = hedgeOptions;
    }

    @Override
    public abstract TaggableReadPreference withTagSet(TagSet tagSet);

    @Override
    public abstract TaggableReadPreference withTagSetList(List<TagSet> tagSet);

    @Override
    public abstract TaggableReadPreference withMaxStalenessMS(Long maxStalenessMS, TimeUnit timeUnit);

    @Override
    public abstract TaggableReadPreference withHedgeOptions(ReadPreferenceHedgeOptions hedgeOptions);

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

        if (hedgeOptions != null) {
            readPrefObject.put("hedge", hedgeOptions.toBsonDocument());
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
    @Nullable
    public Long getMaxStaleness(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        if (maxStalenessMS == null) {
            return null;
        }
        return timeUnit.convert(maxStalenessMS, TimeUnit.MILLISECONDS);
    }

    /**
     * Gets the hedge options.
     *
     * @return the hedge options
     * @mongodb.server.release 4.4
     * @since 4.1
     */
    @Nullable
    public ReadPreferenceHedgeOptions getHedgeOptions() {
        return hedgeOptions;
    }

    @Override
    public String toString() {
        return "ReadPreference{"
                       + "name=" + getName()
                       + (tagSetList.isEmpty() ? "" : ", tagSetList=" + tagSetList)
                       + (maxStalenessMS == null ? "" : ", maxStalenessMS=" + maxStalenessMS)
                       + ", hedgeOptions=" + hedgeOptions
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
        if (hedgeOptions != null ? !hedgeOptions.equals(that.hedgeOptions) : that.hedgeOptions != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = tagSetList.hashCode();
        result = 31 * result + getName().hashCode();
        result = 31 * result + (maxStalenessMS != null ? maxStalenessMS.hashCode() : 0);
        result = 31 * result + (hedgeOptions != null ?  hedgeOptions.hashCode() : 0);
        return result;
    }

    @Override
    protected List<ServerDescription> chooseForNonReplicaSet(final ClusterDescription clusterDescription) {
        return selectFreshServers(clusterDescription, getAny(clusterDescription));
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
        Long maxStaleness = getMaxStaleness(MILLISECONDS);
        if (maxStaleness == null) {
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

        if (maxStaleness < Math.max(SMALLEST_MAX_STALENESS_MS, heartbeatFrequencyMS + IDLE_WRITE_PERIOD_MS)) {
            if (SMALLEST_MAX_STALENESS_MS > heartbeatFrequencyMS + IDLE_WRITE_PERIOD_MS){
                throw new MongoConfigurationException(format("Max staleness (%d sec) must be at least 90 seconds",
                        getMaxStaleness(SECONDS)));
            } else {
                throw new MongoConfigurationException(format("Max staleness (%d ms) must be at least the heartbeat period (%d ms) "
                                                                     + "plus the idle write period (%d ms)",
                        maxStaleness, heartbeatFrequencyMS, IDLE_WRITE_PERIOD_MS));
            }
        }
        List<ServerDescription> freshServers = new ArrayList<ServerDescription>(servers.size());

        ServerDescription primary = findPrimary(clusterDescription);

        if (primary != null) {
            for (ServerDescription cur : servers) {
                if (cur.isPrimary()) {
                    freshServers.add(cur);
                } else {
                    if (getStalenessOfSecondaryRelativeToPrimary(primary, cur, heartbeatFrequencyMS) <= maxStaleness) {
                        freshServers.add(cur);
                    }
                }
            }
        } else {
            ServerDescription mostUpToDateSecondary = findMostUpToDateSecondary(clusterDescription);
            for (ServerDescription cur : servers) {
                if (getLastWriteDateNonNull(mostUpToDateSecondary).getTime() - getLastWriteDateNonNull(cur).getTime()
                        + heartbeatFrequencyMS <= maxStaleness) {
                    freshServers.add(cur);
                }
            }
        }

        return freshServers;
    }

    private long getStalenessOfSecondaryRelativeToPrimary(final ServerDescription primary, final ServerDescription serverDescription,
                                                          final long heartbeatFrequencyMS) {
        return getLastWriteDateNonNull(primary).getTime()
                       + (serverDescription.getLastUpdateTime(MILLISECONDS) - primary.getLastUpdateTime(MILLISECONDS))
                       - getLastWriteDateNonNull(serverDescription).getTime() + heartbeatFrequencyMS;
    }

    @Nullable
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
                            || getLastWriteDateNonNull(cur).getTime() > getLastWriteDateNonNull(mostUpdateToDateSecondary).getTime()) {
                    mostUpdateToDateSecondary = cur;
                }
            }
        }
        if (mostUpdateToDateSecondary == null) {
            throw new MongoInternalException("Expected at least one secondary in cluster description: " + clusterDescription);
        }
        return mostUpdateToDateSecondary;
    }

    private Date getLastWriteDateNonNull(final ServerDescription serverDescription) {
        Date lastWriteDate = serverDescription.getLastWriteDate();
        if (lastWriteDate == null) {
            throw new MongoClientException("lastWriteDate should not be null in " + serverDescription);
        }
        return lastWriteDate;
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

        SecondaryReadPreference(final List<TagSet> tagSetList, @Nullable final Long maxStaleness, final TimeUnit timeUnit) {
            this(tagSetList, maxStaleness, timeUnit, null);
        }

        SecondaryReadPreference(final List<TagSet> tagSetList, @Nullable final Long maxStaleness, final TimeUnit timeUnit,
                                @Nullable final ReadPreferenceHedgeOptions hedgeOptions) {
            super(tagSetList, maxStaleness, timeUnit, hedgeOptions);
        }

        @Override
        public TaggableReadPreference withTagSet(final TagSet tagSet) {
            return withTagSetList(singletonList(tagSet));
        }

        @Override
        public TaggableReadPreference withTagSetList(final List<TagSet> tagSetList) {
            notNull("tagSetList", tagSetList);
            return new SecondaryReadPreference(tagSetList, getMaxStaleness(MILLISECONDS), MILLISECONDS, getHedgeOptions());
        }

        @Override
        public TaggableReadPreference withMaxStalenessMS(@Nullable final Long maxStaleness, final TimeUnit timeUnit) {
            isTrueArgument("maxStaleness is null or >= 0", maxStaleness == null || maxStaleness >= 0);
            return new SecondaryReadPreference(getTagSetList(), maxStaleness, timeUnit, getHedgeOptions());
        }

        @Override
        public TaggableReadPreference withHedgeOptions(final ReadPreferenceHedgeOptions hedgeOptions) {
            return new SecondaryReadPreference(getTagSetList(), getMaxStaleness(MILLISECONDS), MILLISECONDS, hedgeOptions);
        }

        @Override
        public String getName() {
            return "secondary";
        }

        @Override
        protected List<ServerDescription> chooseForReplicaSet(final ClusterDescription clusterDescription) {
            List<ServerDescription> selectedServers = selectFreshServers(clusterDescription, getSecondaries(clusterDescription));
            if (!getTagSetList().isEmpty()) {
                ClusterDescription nonStaleClusterDescription = copyClusterDescription(clusterDescription, selectedServers);
                selectedServers = Collections.emptyList();
                for (final TagSet tagSet : getTagSetList()) {
                    List<ServerDescription> servers = getSecondaries(nonStaleClusterDescription, tagSet);
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

        SecondaryPreferredReadPreference(final List<TagSet> tagSetList, @Nullable final Long maxStaleness, final TimeUnit timeUnit) {
            this(tagSetList, maxStaleness, timeUnit, null);
        }

        SecondaryPreferredReadPreference(final List<TagSet> tagSetList, @Nullable final Long maxStaleness, final TimeUnit timeUnit,
                                         @Nullable final ReadPreferenceHedgeOptions hedgeOptions) {
            super(tagSetList, maxStaleness, timeUnit, hedgeOptions);
        }

        @Override
        public TaggableReadPreference withTagSet(final TagSet tagSet) {
            return withTagSetList(singletonList(tagSet));
        }

        @Override
        public TaggableReadPreference withTagSetList(final List<TagSet> tagSetList) {
            notNull("tagSetList", tagSetList);
            return new SecondaryPreferredReadPreference(tagSetList, getMaxStaleness(MILLISECONDS), MILLISECONDS, getHedgeOptions());
        }

        @Override
        public TaggableReadPreference withMaxStalenessMS(@Nullable final Long maxStaleness, final TimeUnit timeUnit) {
            isTrueArgument("maxStaleness is null or >= 0", maxStaleness == null || maxStaleness >= 0);
            return new SecondaryPreferredReadPreference(getTagSetList(), maxStaleness, timeUnit, getHedgeOptions());
        }

        @Override
        public TaggableReadPreference withHedgeOptions(final ReadPreferenceHedgeOptions hedgeOptions) {
            return new SecondaryPreferredReadPreference(getTagSetList(), getMaxStaleness(MILLISECONDS), MILLISECONDS, hedgeOptions);
        }

        @Override
        public String getName() {
            return "secondaryPreferred";
        }

        @Override
        protected List<ServerDescription> chooseForReplicaSet(final ClusterDescription clusterDescription) {
            List<ServerDescription> selectedServers = super.chooseForReplicaSet(clusterDescription);
            if (selectedServers.isEmpty()) {
                selectedServers = getPrimaries(clusterDescription);
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

        NearestReadPreference(final List<TagSet> tagSetList, @Nullable final Long maxStaleness, final TimeUnit timeUnit) {
            this(tagSetList, maxStaleness, timeUnit, null);
        }

        NearestReadPreference(final List<TagSet> tagSetList, @Nullable final Long maxStaleness, final TimeUnit timeUnit,
                              @Nullable final ReadPreferenceHedgeOptions hedgeOptions) {
            super(tagSetList, maxStaleness, timeUnit, hedgeOptions);
        }

        @Override
        public TaggableReadPreference withTagSet(final TagSet tagSet) {
            return withTagSetList(singletonList(tagSet));
        }

        @Override
        public TaggableReadPreference withTagSetList(final List<TagSet> tagSetList) {
            notNull("tagSetList", tagSetList);
            return new NearestReadPreference(tagSetList, getMaxStaleness(MILLISECONDS), MILLISECONDS, getHedgeOptions());
        }

        @Override
        public TaggableReadPreference withMaxStalenessMS(@Nullable final Long maxStaleness, final TimeUnit timeUnit) {
            isTrueArgument("maxStaleness is null or >= 0", maxStaleness == null || maxStaleness >= 0);
            return new NearestReadPreference(getTagSetList(), maxStaleness, timeUnit, getHedgeOptions());
        }

        @Override
        public TaggableReadPreference withHedgeOptions(final ReadPreferenceHedgeOptions hedgeOptions) {
            return new NearestReadPreference(getTagSetList(), getMaxStaleness(MILLISECONDS), MILLISECONDS, hedgeOptions);
        }

        @Override
        public String getName() {
            return "nearest";
        }


        @Override
        public List<ServerDescription> chooseForReplicaSet(final ClusterDescription clusterDescription) {
            List<ServerDescription> selectedServers = selectFreshServers(clusterDescription, getAnyPrimaryOrSecondary(clusterDescription));
            if (!getTagSetList().isEmpty()) {
                ClusterDescription nonStaleClusterDescription = copyClusterDescription(clusterDescription, selectedServers);
                selectedServers = Collections.emptyList();
                for (final TagSet tagSet : getTagSetList()) {
                    List<ServerDescription> servers = getAnyPrimaryOrSecondary(nonStaleClusterDescription, tagSet);
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

        PrimaryPreferredReadPreference(final List<TagSet> tagSetList, @Nullable final Long maxStaleness, final TimeUnit timeUnit) {
            this(tagSetList, maxStaleness, timeUnit, null);
        }

        PrimaryPreferredReadPreference(final List<TagSet> tagSetList, @Nullable final Long maxStaleness, final TimeUnit timeUnit,
                                       @Nullable final ReadPreferenceHedgeOptions hedgeOptions) {
            super(tagSetList, maxStaleness, timeUnit, hedgeOptions);
        }

        @Override
        public TaggableReadPreference withTagSet(final TagSet tagSet) {
            return withTagSetList(singletonList(tagSet));
        }

        @Override
        public TaggableReadPreference withTagSetList(final List<TagSet> tagSetList) {
            notNull("tagSetList", tagSetList);
            return new PrimaryPreferredReadPreference(tagSetList, getMaxStaleness(MILLISECONDS), MILLISECONDS, getHedgeOptions());
        }

        @Override
        public TaggableReadPreference withMaxStalenessMS(@Nullable final Long maxStaleness, final TimeUnit timeUnit) {
            isTrueArgument("maxStaleness is null or >= 0", maxStaleness == null || maxStaleness >= 0);
            return new PrimaryPreferredReadPreference(getTagSetList(), maxStaleness, timeUnit, getHedgeOptions());
        }

        @Override
        public TaggableReadPreference withHedgeOptions(final ReadPreferenceHedgeOptions hedgeOptions) {
            return new PrimaryPreferredReadPreference(getTagSetList(), getMaxStaleness(MILLISECONDS), MILLISECONDS, hedgeOptions);
        }

        @Override
        public String getName() {
            return "primaryPreferred";
        }

        @Override
        protected List<ServerDescription> chooseForReplicaSet(final ClusterDescription clusterDescription) {
            List<ServerDescription> selectedServers = selectFreshServers(clusterDescription, getPrimaries(clusterDescription));
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
