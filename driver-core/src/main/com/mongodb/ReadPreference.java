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
import com.mongodb.connection.ServerDescription;
import org.bson.BsonDocument;
import org.bson.BsonString;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
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
     * @param maxStaleness the max allowable staleness of secondaries.  A zero value indicates the absence of a maximum.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads primary if available.
     * @since 3.4
     */
    public static ReadPreference primaryPreferred(final long maxStaleness, final TimeUnit timeUnit) {
        return new TaggableReadPreference.PrimaryPreferredReadPreference(Collections.<TagSet>emptyList(),
                                                                                MILLISECONDS.convert(maxStaleness, timeUnit));
    }

    /**
     * Gets a read preference that forces reads to a secondary that is less stale than the given maximum.
     *
     * <p>
     * The driver estimates the staleness of each secondary, based on lastWriteDate values provided in server isMaster responses,
     * and selects only those secondaries whose staleness is less than or equal to maxStaleness.
     * </p>
     *
     * @param maxStaleness the max allowable staleness of secondaries.  A zero value indicates the absence of a maximum.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads secondary.
     * @since 3.4
     */
    public static ReadPreference secondary(final long maxStaleness, final TimeUnit timeUnit) {
        return new TaggableReadPreference.SecondaryReadPreference(Collections.<TagSet>emptyList(),
                                                                         MILLISECONDS.convert(maxStaleness, timeUnit));
    }

    /**
     * Gets a read preference that forces reads to a secondary that is less stale than the given maximumm if one is available,
     * otherwise to the primary.
     *
     * <p>
     * The driver estimates the staleness of each secondary, based on lastWriteDate values provided in server isMaster responses,
     * and selects only those secondaries whose staleness is less than or equal to maxStaleness.
     * </p>     *
     * @param maxStaleness the max allowable staleness of secondaries.  A zero value indicates the absence of a maximum.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads secondary if available, otherwise from primary.
     * @since 3.4
     */
    public static ReadPreference secondaryPreferred(final long maxStaleness, final TimeUnit timeUnit) {
        return new TaggableReadPreference.SecondaryPreferredReadPreference(Collections.<TagSet>emptyList(),
                                                                                MILLISECONDS.convert(maxStaleness, timeUnit));
    }

    /**
     * Gets a read preference that forces reads to a primary or a secondary that is less stale than the given maximum.
     *
     * <p>
     * The driver estimates the staleness of each secondary, based on lastWriteDate values provided in server isMaster responses,
     * and selects only those secondaries whose staleness is less than or equal to maxStaleness.
     * </p>
     *
     * @param maxStaleness the max allowable staleness of secondaries.  A zero value indicates the absence of a maximum.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads nearest
     * @since 3.4
     */
    public static ReadPreference nearest(final long maxStaleness, final TimeUnit timeUnit) {
        return new TaggableReadPreference.NearestReadPreference(Collections.<TagSet>emptyList(),
                                                                                MILLISECONDS.convert(maxStaleness, timeUnit));
    }

    /**
     * Gets a read preference that forces reads to the primary if available, otherwise to a secondary with the given set of tags.
     *
     * @param tagSet the set of tags to limit the list of secondaries to.
     * @return ReadPreference which reads primary if available, otherwise a secondary respective of tags.\
     * @since 2.13
     */
    public static TaggableReadPreference primaryPreferred(final TagSet tagSet) {
        return primaryPreferred(tagSet, 0, MILLISECONDS);
    }

    /**
     * Gets a read preference that forces reads to a secondary with the given set of tags.
     *
     * @param tagSet the set of tags to limit the list of secondaries to
     * @return ReadPreference which reads secondary respective of tags.
     * @since 2.13
     */
    public static TaggableReadPreference secondary(final TagSet tagSet) {
        return secondary(tagSet, 0, MILLISECONDS);
    }

    /**
     * Gets a read preference that forces reads to a secondary with the given set of tags, or the primary is none are available.
     *
     * @param tagSet the set of tags to limit the list of secondaries to
     * @return ReadPreference which reads secondary if available respective of tags, otherwise from primary irrespective of tags.
     * @since 2.13
     */
    public static TaggableReadPreference secondaryPreferred(final TagSet tagSet) {
        return secondaryPreferred(tagSet, 0, MILLISECONDS);
    }

    /**
     * Gets a read preference that forces reads to the primary or a secondary with the given set of tags.
     *
     * @param tagSet the set of tags to limit the list of secondaries to
     * @return ReadPreference which reads nearest node respective of tags.
     * @since 2.13
     */
    public static TaggableReadPreference nearest(final TagSet tagSet) {
        return nearest(tagSet, 0, MILLISECONDS);
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
     * @param maxStaleness the max allowable staleness of secondaries.  A zero value indicates the absence of a maximum.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads primary if available, otherwise a secondary respective of tags.\
     * @since 3.4
     */
    public static TaggableReadPreference primaryPreferred(final TagSet tagSet,
                                                          final long maxStaleness, final TimeUnit timeUnit) {
        return new TaggableReadPreference.PrimaryPreferredReadPreference(Collections.singletonList(tagSet),
                                                                                MILLISECONDS.convert(maxStaleness, timeUnit));
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
     * @param maxStaleness the max allowable staleness of secondaries.  A zero value indicates the absence of a maximum.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads secondary respective of tags.
     * @since 3.4
     */
    public static TaggableReadPreference secondary(final TagSet tagSet,
                                                   final long maxStaleness, final TimeUnit timeUnit) {
        return new TaggableReadPreference.SecondaryReadPreference(Collections.singletonList(tagSet),
                                                                         MILLISECONDS.convert(maxStaleness, timeUnit));
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
     * @param maxStaleness the max allowable staleness of secondaries.  A zero value indicates the absence of a maximum.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads secondary if available respective of tags, otherwise from primary irrespective of tags.
     * @since 3.4
     */
    public static TaggableReadPreference secondaryPreferred(final TagSet tagSet,
                                                            final long maxStaleness, final TimeUnit timeUnit) {
        return new TaggableReadPreference.SecondaryPreferredReadPreference(Collections.singletonList(tagSet),
                                                                                  MILLISECONDS.convert(maxStaleness, timeUnit));
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
     * @param maxStaleness the max allowable staleness of secondaries.  A zero value indicates the absence of a maximum.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads nearest node respective of tags.
     * @since 3.4
     */
    public static TaggableReadPreference nearest(final TagSet tagSet,
                                                 final long maxStaleness, final TimeUnit timeUnit) {
        return new TaggableReadPreference.NearestReadPreference(Collections.singletonList(tagSet),
                                                                       MILLISECONDS.convert(maxStaleness, timeUnit));
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
        return primaryPreferred(tagSetList, 0, MILLISECONDS);
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
        return secondary(tagSetList, 0, MILLISECONDS);
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
        return secondaryPreferred(tagSetList, 0, MILLISECONDS);
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
        return nearest(tagSetList, 0, MILLISECONDS);
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
     * @param maxStaleness the max allowable staleness of secondaries.  A zero value indicates the absence of a maximum.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads primary if available, otherwise a secondary respective of tags.
     * @since 3.4
     */
    public static TaggableReadPreference primaryPreferred(final List<TagSet> tagSetList,
                                                          final long maxStaleness, final TimeUnit timeUnit) {
        return new TaggableReadPreference.PrimaryPreferredReadPreference(tagSetList, MILLISECONDS.convert(maxStaleness, timeUnit));
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
     * @param maxStaleness the max allowable staleness of secondaries.  A zero value indicates the absence of a maximum.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads secondary respective of tags.
     * @since 3.4
     */
    public static TaggableReadPreference secondary(final List<TagSet> tagSetList,
                                                   final long maxStaleness, final TimeUnit timeUnit) {
        return new TaggableReadPreference.SecondaryReadPreference(tagSetList, MILLISECONDS.convert(maxStaleness, timeUnit));
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
     * @param maxStaleness the max allowable staleness of secondaries.  A zero value indicates the absence of a maximum.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads secondary if available respective of tags, otherwise from primary irrespective of tags.
     * @since 3.4
     */
    public static TaggableReadPreference secondaryPreferred(final List<TagSet> tagSetList,
                                                            final long maxStaleness, final TimeUnit timeUnit) {
        return new TaggableReadPreference.SecondaryPreferredReadPreference(tagSetList, MILLISECONDS.convert(maxStaleness, timeUnit));
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
     * @param maxStaleness the max allowable staleness of secondaries.  A zero value indicates the absence of a maximum.
     * @param timeUnit the time unit of maxStaleness
     * @return ReadPreference which reads nearest node respective of tags.
     * @since 3.4
     */
    public static TaggableReadPreference nearest(final List<TagSet> tagSetList,
                                                 final long maxStaleness, final TimeUnit timeUnit) {
        return new TaggableReadPreference.NearestReadPreference(tagSetList, MILLISECONDS.convert(maxStaleness, timeUnit));
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
        return valueOf(name, tagSetList, 0, MILLISECONDS);
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
     * @param maxStaleness the max allowable staleness of secondaries.  A zero value indicates the absence of a maximum.
     * @param timeUnit the time unit of maxStaleness
     * @return the taggable read preference
     * @since 3.4
     */
    public static TaggableReadPreference valueOf(final String name, final List<TagSet> tagSetList, final long maxStaleness,
                                                 final TimeUnit timeUnit) {
        notNull("name", name);
        notNull("tagSetList", tagSetList);
        notNull("timeUnit", timeUnit);

        String nameToCheck = name.toLowerCase();

        if (nameToCheck.equals(PRIMARY.getName().toLowerCase())) {
            throw new IllegalArgumentException("Primary read preference can not also specify tag sets or max staleness");
        }

        long maxStalenessMS = MILLISECONDS.convert(maxStaleness, timeUnit);

        if (nameToCheck.equals(SECONDARY.getName().toLowerCase())) {
            return new TaggableReadPreference.SecondaryReadPreference(tagSetList, maxStalenessMS);
        }
        if (nameToCheck.equals(SECONDARY_PREFERRED.getName().toLowerCase())) {
            return new TaggableReadPreference.SecondaryPreferredReadPreference(tagSetList, maxStalenessMS);
        }
        if (nameToCheck.equals(PRIMARY_PREFERRED.getName().toLowerCase())) {
            return new TaggableReadPreference.PrimaryPreferredReadPreference(tagSetList, maxStalenessMS);
        }
        if (nameToCheck.equals(NEAREST.getName().toLowerCase())) {
            return new TaggableReadPreference.NearestReadPreference(tagSetList, maxStalenessMS);
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
        SECONDARY = new TaggableReadPreference.SecondaryReadPreference();
        SECONDARY_PREFERRED = new TaggableReadPreference.SecondaryPreferredReadPreference();
        PRIMARY_PREFERRED = new TaggableReadPreference.PrimaryPreferredReadPreference();
        NEAREST = new TaggableReadPreference.NearestReadPreference();
   }
}
