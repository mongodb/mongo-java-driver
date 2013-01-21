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

package org.mongodb;

import org.bson.types.Document;
import org.mongodb.annotations.Immutable;
import org.mongodb.rs.ReplicaSet;
import org.mongodb.rs.ReplicaSetNode;

/**
 * An abstract class that represents preferred replica set members to which a query or command can be sent.
 *
 * @mongodb.driver.manual applications/replication/#replica-set-read-preference  Read Preference
 */
@Immutable
public abstract class ReadPreference implements ConvertibleToDocument {

    ReadPreference() {
    }

    /**
     * @return <code>true</code> if this preference allows reads or commands from secondary nodes
     */
    public abstract boolean isSlaveOk();

    /**
     * @return <code>DBObject</code> representation of this preference
     */
    @Override
    public abstract Document toDocument();

    /**
     * The name of this read preference.
     *
     * @return the name
     */
    public abstract String getName();

    abstract ReplicaSetNode getNode(ReplicaSet set);

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

        @Override
        public ReplicaSetNode getNode(final ReplicaSet set) {
            return set.getMaster();
        }

        @Override
        public Document toDocument() {
            return new Document("mode", getName());
        }

        @Override
        public String getName() {
            return "primary";
        }
    }


    /**
     * @return ReadPreference which reads from primary only
     */
    public static ReadPreference primary() {
        return PRIMARY;
    }

    /**
     * @return ReadPreference which reads primary if available.
     */
    public static ReadPreference primaryPreferred() {
        return PRIMARY_PREFERRED;
    }

    /**
     * @return ReadPreference which reads primary if available, otherwise a secondary respective of tags.
     */
    public static TaggableReadPreference primaryPreferred(final Document firstTagSet, final Document... remainingTagSets) {
        return new TaggableReadPreference.PrimaryPreferredReadPreference(firstTagSet, remainingTagSets);
    }

    /**
     * @return ReadPreference which reads secondary.
     */
    public static ReadPreference secondary() {
        return SECONDARY;
    }

    /**
     * @return ReadPreference which reads secondary respective of tags.
     */
    public static TaggableReadPreference secondary(final Document firstTagSet, final Document... remainingTagSets) {
        return new TaggableReadPreference.SecondaryReadPreference(firstTagSet, remainingTagSets);
    }

    /**
     * @return ReadPreference which reads secondary if available, otherwise from primary.
     */
    public static ReadPreference secondaryPreferred() {
        return SECONDARY_PREFERRED;
    }

    /**
     * @return ReadPreference which reads secondary if available respective of tags, otherwise from primary irrespective
     *         of tags.
     */
    public static TaggableReadPreference secondaryPreferred(final Document firstTagSet, final Document... remainingTagSets) {
        return new TaggableReadPreference.SecondaryPreferredReadPreference(firstTagSet, remainingTagSets);
    }

    /**
     * @return ReadPreference which reads nearest node.
     */
    public static ReadPreference nearest() {
        return NEAREST;
    }

    public static ReadPreference valueOf(final String name) {
        if (name == null) {
            throw new IllegalArgumentException();
        }

        final String nameToCheck = name.toLowerCase();

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

    public static TaggableReadPreference valueOf(final String name, final Document firstTagSet,
                                                 final Document... remainingTagSets) {
        if (name == null) {
            throw new IllegalArgumentException("Name cannot be null");
        }

        final String nameToCheck = name.toLowerCase();

        if (nameToCheck.equals(SECONDARY.getName().toLowerCase())) {
            return new TaggableReadPreference.SecondaryReadPreference(firstTagSet, remainingTagSets);
        }
        if (nameToCheck.equals(SECONDARY_PREFERRED.getName().toLowerCase())) {
            return new TaggableReadPreference.SecondaryPreferredReadPreference(firstTagSet, remainingTagSets);
        }
        if (nameToCheck.equals(PRIMARY_PREFERRED.getName().toLowerCase())) {
            return new TaggableReadPreference.PrimaryPreferredReadPreference(firstTagSet, remainingTagSets);
        }
        if (nameToCheck.equals(NEAREST.getName().toLowerCase())) {
            return new TaggableReadPreference.NearestReadPreference(firstTagSet, remainingTagSets);
        }

        throw new IllegalArgumentException("No match for read preference of " + name);
    }

    /**
     * @return ReadPreference which reads nearest node respective of tags.
     */
    public static TaggableReadPreference nearest(final Document firstTagSet,
                                                 final Document... remainingTagSets) {
        return new TaggableReadPreference.NearestReadPreference(firstTagSet, remainingTagSets);
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

