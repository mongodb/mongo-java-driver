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

import com.mongodb.TaggableReadPreference.NearestReadPreference;
import com.mongodb.TaggableReadPreference.PrimaryPreferredReadPreference;
import com.mongodb.TaggableReadPreference.SecondaryPreferredReadPreference;
import com.mongodb.TaggableReadPreference.SecondaryReadPreference;
import com.mongodb.annotations.Immutable;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import org.bson.BsonDocument;
import org.bson.BsonString;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A class that represents preferred replica set members to which a query or command can be sent.
 *
 * @mongodb.driver.manual core/read-preference  Read Preference
 */
@Immutable
public abstract class ReadPreference {

    ReadPreference() {
    }

    /**
     * True if this read preference allows reading from a secondary member of a replica set.
     *
     * @return if reading from a secondary is ok
     */
    public abstract boolean isSlaveOk();

    /**
     * Gets the name of this read preference.
     *
     * @return the name
     */
    public abstract String getName();

    /**
     * Gets a document representing this read preference in the wire protocol.
     *
     * @return the document
     */
    public abstract BsonDocument toDocument();

    /**
     * Chooses the servers from the given cluster than match this read preference.
     *
     * @param clusterDescription the cluster description
     * @return a list of matching server descriptions, which may be empty but may not be null
     */
    public final List<ServerDescription> choose(final ClusterDescription clusterDescription) {
        switch (clusterDescription.getType()) {
            case REPLICA_SET:
                return chooseForReplicaSet(clusterDescription);
            case SHARDED:
            case STANDALONE:
                return chooseForNonReplicaSet(clusterDescription);
            case UNKNOWN:
                return Collections.emptyList();
            default:
                throw new UnsupportedOperationException("Unsupported cluster type: " + clusterDescription.getType());
        }
    }

    protected abstract List<ServerDescription> chooseForNonReplicaSet(final ClusterDescription clusterDescription);

    protected abstract List<ServerDescription> chooseForReplicaSet(final ClusterDescription clusterDescription);

    /**
     * Gets a read preference that forces read to the primary.
     *
     * @return ReadPreference which reads from primary only
     */
    public static ReadPreference primary() {
        return PRIMARY;
    }

    /**
     * Gets a read preference that forces reads to the primary if available, otherwise to a secondary.
     *
     * @return ReadPreference which reads primary if available.
     */
    public static ReadPreference primaryPreferred() {
        return PRIMARY_PREFERRED;
    }

    /**
     * Gets a read preference that forces reads to a secondary.
     *
     * @return ReadPreference which reads secondary.
     */
    public static ReadPreference secondary() {
        return SECONDARY;
    }

    /**
     * Gets a read preference that forces reads to a secondary if one is available, otherwise to the primary.
     *
     * @return ReadPreference which reads secondary if available, otherwise from primary.
     */
    public static ReadPreference secondaryPreferred() {
        return SECONDARY_PREFERRED;
    }

    /**
     * Gets a read preference that forces reads to a primary or a secondary.
     *
     * @return ReadPreference which reads nearest
     */
    public static ReadPreference nearest() {
        return NEAREST;
    }

    /**
     * Gets a read preference that forces reads to the primary if available, otherwise to a secondary.
     *
     * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
     *                     plus 10 seconds, whichever is greatest.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads primary if available.
     * @since 3.4
     * @see TaggableReadPreference#getMaxStaleness(TimeUnit)
     */
    public static ReadPreference primaryPreferred(final long maxStaleness, final TimeUnit timeUnit) {
        return new PrimaryPreferredReadPreference(Collections.<TagSet>emptyList(), maxStaleness, timeUnit);
    }

    /**
     * Gets a read preference that forces reads to a secondary that is less stale than the given maximum.
     *
     * <p>
     * The driver estimates the staleness of each secondary, based on lastWriteDate values provided in server isMaster responses,
     * and selects only those secondaries whose staleness is less than or equal to maxStaleness.
     * </p>
     *
     * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
     *                     plus 10 seconds, whichever is greatest.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads secondary.
     * @since 3.4
     * @see TaggableReadPreference#getMaxStaleness(TimeUnit)
     */
    public static ReadPreference secondary(final long maxStaleness, final TimeUnit timeUnit) {
        return new SecondaryReadPreference(Collections.<TagSet>emptyList(), maxStaleness, timeUnit);
    }

    /**
     * Gets a read preference that forces reads to a secondary that is less stale than the given maximumm if one is available,
     * otherwise to the primary.
     *
     * <p>
     * The driver estimates the staleness of each secondary, based on lastWriteDate values provided in server isMaster responses,
     * and selects only those secondaries whose staleness is less than or equal to maxStaleness.
     * </p>     *
     * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
     *                     plus 10 seconds, whichever is greatest.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads secondary if available, otherwise from primary.
     * @since 3.4
     * @see TaggableReadPreference#getMaxStaleness(TimeUnit)
     */
    public static ReadPreference secondaryPreferred(final long maxStaleness, final TimeUnit timeUnit) {
        return new SecondaryPreferredReadPreference(Collections.<TagSet>emptyList(), maxStaleness, timeUnit);
    }

    /**
     * Gets a read preference that forces reads to a primary or a secondary that is less stale than the given maximum.
     *
     * <p>
     * The driver estimates the staleness of each secondary, based on lastWriteDate values provided in server isMaster responses,
     * and selects only those secondaries whose staleness is less than or equal to maxStaleness.
     * </p>
     *
     * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
     *                     plus 10 seconds, whichever is greatest.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads nearest
     * @since 3.4
     * @see TaggableReadPreference#getMaxStaleness(TimeUnit)
     */
    public static ReadPreference nearest(final long maxStaleness, final TimeUnit timeUnit) {
        return new NearestReadPreference(Collections.<TagSet>emptyList(), maxStaleness, timeUnit);
    }

    /**
     * Gets a read preference that forces reads to the primary if available, otherwise to a secondary with the given set of tags.
     *
     * @param tagSet the set of tags to limit the list of secondaries to.
     * @return ReadPreference which reads primary if available, otherwise a secondary respective of tags.\
     * @since 2.13
     */
    public static TaggableReadPreference primaryPreferred(final TagSet tagSet) {
        return new PrimaryPreferredReadPreference(singletonList(tagSet), null, MILLISECONDS);
    }

    /**
     * Gets a read preference that forces reads to a secondary with the given set of tags.
     *
     * @param tagSet the set of tags to limit the list of secondaries to
     * @return ReadPreference which reads secondary respective of tags.
     * @since 2.13
     */
    public static TaggableReadPreference secondary(final TagSet tagSet) {
        return new SecondaryReadPreference(singletonList(tagSet), null, MILLISECONDS);
    }

    /**
     * Gets a read preference that forces reads to a secondary with the given set of tags, or the primary is none are available.
     *
     * @param tagSet the set of tags to limit the list of secondaries to
     * @return ReadPreference which reads secondary if available respective of tags, otherwise from primary irrespective of tags.
     * @since 2.13
     */
    public static TaggableReadPreference secondaryPreferred(final TagSet tagSet) {
        return new SecondaryPreferredReadPreference(singletonList(tagSet), null, MILLISECONDS);
    }

    /**
     * Gets a read preference that forces reads to the primary or a secondary with the given set of tags.
     *
     * @param tagSet the set of tags to limit the list of secondaries to
     * @return ReadPreference which reads nearest node respective of tags.
     * @since 2.13
     */
    public static TaggableReadPreference nearest(final TagSet tagSet) {
        return new NearestReadPreference(singletonList(tagSet), null, MILLISECONDS);
    }

    /**
     * Gets a read preference that forces reads to the primary if available, otherwise to a secondary with the given set of tags
     * that is less stale than the given maximum.
     *
     * <p>
     * The driver estimates the staleness of each secondary, based on lastWriteDate values provided in server isMaster responses,
     * and selects only those secondaries whose staleness is less than or equal to maxStaleness.
     * </p>
     *
     * @param tagSet the set of tags to limit the list of secondaries to.
     * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
     *                     plus 10 seconds, whichever is greatest.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads primary if available, otherwise a secondary respective of tags.\
     * @since 3.4
     * @see TaggableReadPreference#getMaxStaleness(TimeUnit)
     */
    public static TaggableReadPreference primaryPreferred(final TagSet tagSet,
                                                          final long maxStaleness, final TimeUnit timeUnit) {
        return new PrimaryPreferredReadPreference(singletonList(tagSet), maxStaleness, timeUnit);
    }

    /**
     * Gets a read preference that forces reads to a secondary with the given set of tags that is less stale than the given maximum.
     *
     * <p>
     * The driver estimates the staleness of each secondary, based on lastWriteDate values provided in server isMaster responses,
     * and selects only those secondaries whose staleness is less than or equal to maxStaleness.
     * </p>
     *
     * @param tagSet the set of tags to limit the list of secondaries to
     * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
     *                     plus 10 seconds, whichever is greatest.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads secondary respective of tags.
     * @since 3.4
     * @see TaggableReadPreference#getMaxStaleness(TimeUnit)
     */
    public static TaggableReadPreference secondary(final TagSet tagSet,
                                                   final long maxStaleness, final TimeUnit timeUnit) {
        return new SecondaryReadPreference(singletonList(tagSet), maxStaleness, timeUnit);
    }

    /**
     * Gets a read preference that forces reads to a secondary with the given set of tags that is less stale than the given maximum,
     * or the primary is none are available.
     *
     * <p>
     * The driver estimates the staleness of each secondary, based on lastWriteDate values provided in server isMaster responses,
     * and selects only those secondaries whose staleness is less than or equal to maxStaleness.
     * </p>     *
     * @param tagSet the set of tags to limit the list of secondaries to
     * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
     *                     plus 10 seconds, whichever is greatest.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads secondary if available respective of tags, otherwise from primary irrespective of tags.
     * @since 3.4
     * @see TaggableReadPreference#getMaxStaleness(TimeUnit)
     */
    public static TaggableReadPreference secondaryPreferred(final TagSet tagSet,
                                                            final long maxStaleness, final TimeUnit timeUnit) {
        return new SecondaryPreferredReadPreference(singletonList(tagSet), maxStaleness, timeUnit);
    }

    /**
     * Gets a read preference that forces reads to the primary or a secondary with the given set of tags that is less stale than the
     * given maximum.
     *
     * <p>
     * The driver estimates the staleness of each secondary, based on lastWriteDate values provided in server isMaster responses,
     * and selects only those secondaries whose staleness is less than or equal to maxStaleness.
     * </p>
     *
     * @param tagSet the set of tags to limit the list of secondaries to
     * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
     *                     plus 10 seconds, whichever is greatest.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads nearest node respective of tags.
     * @since 3.4
     * @see TaggableReadPreference#getMaxStaleness(TimeUnit)
     */
    public static TaggableReadPreference nearest(final TagSet tagSet,
                                                 final long maxStaleness, final TimeUnit timeUnit) {
        return new NearestReadPreference(singletonList(tagSet), maxStaleness, timeUnit);
    }


    /**
     * Gets a read preference that forces reads to the primary if available, otherwise to a secondary with one of the given sets of tags.
     * The driver will look for a secondary with each tag set in the given list, stopping after one is found,
     * or failing if no secondary can be found that matches any of the tag sets in the list.
     *
     * @param tagSetList the list of tag sets to limit the list of secondaries to
     * @return ReadPreference which reads primary if available, otherwise a secondary respective of tags.
     * @since 2.13
     */
    public static TaggableReadPreference primaryPreferred(final List<TagSet> tagSetList) {
        return new PrimaryPreferredReadPreference(tagSetList, null, MILLISECONDS);
    }

    /**
     * Gets a read preference that forces reads to a secondary with one of the given sets of tags.
     * The driver will look for a secondary with each tag set in the given list, stopping after one is found,
     * or failing if no secondary can be found that matches any of the tag sets in the list.
     *
     * @param tagSetList the list of tag sets to limit the list of secondaries to
     * @return ReadPreference which reads secondary respective of tags.
     * @since 2.13
     */
    public static TaggableReadPreference secondary(final List<TagSet> tagSetList) {
        return new SecondaryReadPreference(tagSetList, null, MILLISECONDS);
    }

    /**
     * Gets a read preference that forces reads to a secondary with one of the given sets of tags.
     * The driver will look for a secondary with each tag set in the given list, stopping after one is found,
     * or the primary if none are available.
     *
     * @param tagSetList the list of tag sets to limit the list of secondaries to
     * @return ReadPreference which reads secondary if available respective of tags, otherwise from primary irrespective of tags.
     * @since 2.13
     */
    public static TaggableReadPreference secondaryPreferred(final List<TagSet> tagSetList) {
        return new SecondaryPreferredReadPreference(tagSetList, null, MILLISECONDS);
    }

    /**
     * Gets a read preference that forces reads to the primary or a secondary with one of the given sets of tags.
     * The driver will look for a secondary with each tag set in the given list, stopping after one is found,
     * or the primary if none are available.
     *
     * @param tagSetList the list of tag sets to limit the list of secondaries to
     * @return ReadPreference which reads nearest node respective of tags.
     * @since 2.13
     */
    public static TaggableReadPreference nearest(final List<TagSet> tagSetList) {
        return new NearestReadPreference(tagSetList, null, MILLISECONDS);
    }

    /**
     * Gets a read preference that forces reads to the primary if available, otherwise to a secondary with one of the given sets of tags
     * that is less stale than the given maximum.
     *
     * <p>
     * The driver will look for a secondary with each tag set in the given list, stopping after one is found,
     * or failing if no secondary can be found that matches any of the tag sets in the list.
     * </p>
     *
     * <p>
     * The driver estimates the staleness of each secondary, based on lastWriteDate values provided in server isMaster responses,
     * and selects only those secondaries whose staleness is less than or equal to maxStaleness.
     * </p>
     *
     * @param tagSetList the list of tag sets to limit the list of secondaries to
     * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
     *                     plus 10 seconds, whichever is greatest.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads primary if available, otherwise a secondary respective of tags.
     * @since 3.4
     * @see TaggableReadPreference#getMaxStaleness(TimeUnit)
     */
    public static TaggableReadPreference primaryPreferred(final List<TagSet> tagSetList,
                                                          final long maxStaleness, final TimeUnit timeUnit) {
        return new PrimaryPreferredReadPreference(tagSetList, maxStaleness, timeUnit);
    }

    /**
     * Gets a read preference that forces reads to a secondary with one of the given sets of tags that is less stale than
     * the given maximum.
     *
     * <p>
     * The driver will look for a secondary with each tag set in the given list, stopping after one is found,
     * or failing if no secondary can be found that matches any of the tag sets in the list.
     * </p>
     *
     * <p>
     * The driver estimates the staleness of each secondary, based on lastWriteDate values provided in server isMaster responses,
     * and selects only those secondaries whose staleness is less than or equal to maxStaleness.
     * </p>
     *
     * @param tagSetList the list of tag sets to limit the list of secondaries to
     * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
     *                     plus 10 seconds, whichever is greatest.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads secondary respective of tags.
     * @since 3.4
     * @see TaggableReadPreference#getMaxStaleness(TimeUnit)
     */
    public static TaggableReadPreference secondary(final List<TagSet> tagSetList,
                                                   final long maxStaleness, final TimeUnit timeUnit) {
        return new SecondaryReadPreference(tagSetList, maxStaleness, timeUnit);
    }

    /**
     * Gets a read preference that forces reads to a secondary with one of the given sets of tags that is less stale than
     * the given maximum.
     *
     * <p>
     * The driver will look for a secondary with each tag set in the given list, stopping after one is found,
     * or the primary if none are available.
     * </p>
     *
     * <p>
     * The driver estimates the staleness of each secondary, based on lastWriteDate values provided in server isMaster responses,
     * and selects only those secondaries whose staleness is less than or equal to maxStaleness.
     * </p>
     *
     * @param tagSetList the list of tag sets to limit the list of secondaries to
     * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
     *                     plus 10 seconds, whichever is greatest.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads secondary if available respective of tags, otherwise from primary irrespective of tags.
     * @since 3.4
     * @see TaggableReadPreference#getMaxStaleness(TimeUnit)
     */
    public static TaggableReadPreference secondaryPreferred(final List<TagSet> tagSetList,
                                                            final long maxStaleness, final TimeUnit timeUnit) {
        return new SecondaryPreferredReadPreference(tagSetList, maxStaleness, timeUnit);
    }

    /**
     * Gets a read preference that forces reads to the primary or a secondary with one of the given sets of tags that is less stale than
     * the given maximum.
     *
     * <p>
     * The driver will look for a secondary with each tag set in the given list, stopping after one is found,
     * or the primary if none are available.
     * </p>
     *
     * <p>
     * The driver estimates the staleness of each secondary, based on lastWriteDate values provided in server isMaster responses,
     * and selects only those secondaries whose staleness is less than or equal to maxStaleness.
     * </p>
     *
     * @param tagSetList the list of tag sets to limit the list of secondaries to
     * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
     *                     plus 10 seconds, whichever is greatest.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads nearest node respective of tags.
     * @since 3.4
     * @see TaggableReadPreference#getMaxStaleness(TimeUnit)
     */
    public static TaggableReadPreference nearest(final List<TagSet> tagSetList,
                                                 final long maxStaleness, final TimeUnit timeUnit) {
        return new NearestReadPreference(tagSetList, maxStaleness, timeUnit);
    }

    /**
     * Creates a read preference from the given read preference name.
     *
     * @param name the name of the read preference
     * @return the read preference
     */
    public static ReadPreference valueOf(final String name) {
        if (name == null) {
            throw new IllegalArgumentException();
        }

        String nameToCheck = name.toLowerCase();

        if (nameToCheck.equals(PRIMARY.getName().toLowerCase())) {
            return PRIMARY;
        }
        if (nameToCheck.equals(SECONDARY.getName().toLowerCase())) {
            return SECONDARY;
        }
        if (nameToCheck.equals(SECONDARY_PREFERRED.getName().toLowerCase())) {
            return SECONDARY_PREFERRED;
        }
        if (nameToCheck.equals(PRIMARY_PREFERRED.getName().toLowerCase())) {
            return PRIMARY_PREFERRED;
        }
        if (nameToCheck.equals(NEAREST.getName().toLowerCase())) {
            return NEAREST;
        }

        throw new IllegalArgumentException("No match for read preference of " + name);
    }

    /**
     * Creates a taggable read preference from the given read preference name and list of tag sets.
     *
     * @param name the name of the read preference
     * @param tagSetList the list of tag sets
     * @return the taggable read preference
     * @since 2.13
     */
    public static TaggableReadPreference valueOf(final String name, final List<TagSet> tagSetList) {
        return valueOf(name, tagSetList, null, MILLISECONDS);
    }

    /**
     * Creates a taggable read preference from the given read preference name, list of tag sets, and max allowable staleness of secondaries.
     *
     * <p>
     * The driver estimates the staleness of each secondary, based on lastWriteDate values provided in server isMaster responses,
     * and selects only those secondaries whose staleness is less than or equal to maxStaleness.
     * </p>
     *
     * @param name the name of the read preference
     * @param tagSetList the list of tag sets
     * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
     *                     plus 10 seconds, whichever is greatest.
     * @param timeUnit the time unit of maxStaleness
     * @return the taggable read preference
     * @since 3.4
     * @see TaggableReadPreference#getMaxStaleness(TimeUnit)
     */
    public static TaggableReadPreference valueOf(final String name, final List<TagSet> tagSetList, final long maxStaleness,
                                                 final TimeUnit timeUnit) {
        return valueOf(name, tagSetList, (Long) maxStaleness, timeUnit);
    }

    private static TaggableReadPreference valueOf(final String name, final List<TagSet> tagSetList, final Long maxStaleness,
                                                  final TimeUnit timeUnit) {
        notNull("name", name);
        notNull("tagSetList", tagSetList);
        notNull("timeUnit", timeUnit);

        String nameToCheck = name.toLowerCase();

        if (nameToCheck.equals(PRIMARY.getName().toLowerCase())) {
            throw new IllegalArgumentException("Primary read preference can not also specify tag sets or max staleness");
        }

        if (nameToCheck.equals(SECONDARY.getName().toLowerCase())) {
            return new SecondaryReadPreference(tagSetList, maxStaleness, timeUnit);
        }
        if (nameToCheck.equals(SECONDARY_PREFERRED.getName().toLowerCase())) {
            return new SecondaryPreferredReadPreference(tagSetList, maxStaleness, timeUnit);
        }
        if (nameToCheck.equals(PRIMARY_PREFERRED.getName().toLowerCase())) {
            return new PrimaryPreferredReadPreference(tagSetList, maxStaleness, timeUnit);
        }
        if (nameToCheck.equals(NEAREST.getName().toLowerCase())) {
            return new NearestReadPreference(tagSetList, maxStaleness, timeUnit);
        }

        throw new IllegalArgumentException("No match for read preference of " + name);
    }

    /**
     * Preference to read from primary only. Cannot be combined with tags.
     */
    private static final class PrimaryReadPreference extends ReadPreference {
        private PrimaryReadPreference() {
        }

        @Override
        public boolean isSlaveOk() {
            return false;
        }

        @Override
        public String toString() {
            return getName();
        }

        @Override
        public boolean equals(final Object o) {
            return o != null && getClass() == o.getClass();
        }

        @Override
        public int hashCode() {
            return getName().hashCode();
        }

        public BsonDocument toDocument() {
            return new BsonDocument("mode", new BsonString(getName()));
        }

        @Override
        @SuppressWarnings("deprecation")
        protected List<ServerDescription> chooseForReplicaSet(final ClusterDescription clusterDescription) {
            return clusterDescription.getPrimaries();
        }

        @Override
        @SuppressWarnings("deprecation")
        protected List<ServerDescription> chooseForNonReplicaSet(final ClusterDescription clusterDescription) {
            return clusterDescription.getAny();
        }

        @Override
        public String getName() {
            return "primary";
        }
    }

    private static final ReadPreference PRIMARY;
    private static final ReadPreference SECONDARY;
    private static final ReadPreference SECONDARY_PREFERRED;
    private static final ReadPreference PRIMARY_PREFERRED;
    private static final ReadPreference NEAREST;

    static {
        PRIMARY = new PrimaryReadPreference();
        SECONDARY = new SecondaryReadPreference();
        SECONDARY_PREFERRED = new SecondaryPreferredReadPreference();
        PRIMARY_PREFERRED = new PrimaryPreferredReadPreference();
        NEAREST = new NearestReadPreference();
   }
}
