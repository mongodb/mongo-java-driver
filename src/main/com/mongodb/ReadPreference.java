/**
 * Copyright (c) 2008 - 2011 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.mongodb;

import com.mongodb.ReplicaSetStatus.ReplicaSetNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * an abstract class that represents preferred replica set members to which a query or command can be sent
 */
public abstract class ReadPreference {

    ReadPreference() {
    }

    /**
     * @return <code>true</code> if this preference allows reads or commands from secondary nodes
     */
    public abstract boolean isSlaveOk();
    abstract ReplicaSetNode getNode(ReplicaSetStatus.ReplicaSet set);

    /**
     * @return <code>DBObject</code> representation of this preference
     */
    public abstract DBObject toDBObject();

    /**
     * The name of this read preference.
     *
     * @return the name
     */
    public abstract String getName();

    /**
     * Preference to read from primary only.
     * Cannot be combined with tags.
     *
     * @author breinero
     */
    private static class PrimaryReadPreference extends ReadPreference {
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
        ReplicaSetNode getNode(ReplicaSetStatus.ReplicaSet set) {
            return set.getMaster();
        }

        @Override
        public DBObject toDBObject() {
            return new BasicDBObject("mode", getName());
        }

        @Override
        public String getName() {
            return "primary";
        }
    }

    /**
     * Read from a secondary if available and matches tags.
     *
     * @deprecated As of release 2.9, replaced by
     * <code>ReadPreference.secondaryPreferred(DBObject firstTagSet, DBObject... remainingTagSets)</code>
     */
    @Deprecated
    public static class TaggedReadPreference extends ReadPreference {

        public TaggedReadPreference(Map<String, String> tags) {
            if (tags == null || tags.size() == 0) {
                throw new IllegalArgumentException("tags can not be null or empty");
            }
            _tags = new BasicDBObject(tags);
            List<DBObject> maps = splitMapIntoMultipleMaps(_tags);
            _pref = new TaggableReadPreference.SecondaryReadPreference(maps.get(0), getRemainingMaps(maps));

        }

        public TaggedReadPreference(DBObject tags) {
            if (tags == null || tags.keySet().size() == 0) {
                throw new IllegalArgumentException("tags can not be null or empty");
            }
            _tags = tags;
            List<DBObject> maps = splitMapIntoMultipleMaps(_tags);
            _pref = new TaggableReadPreference.SecondaryReadPreference(maps.get(0), getRemainingMaps(maps));
        }

        public DBObject getTags() {
            DBObject tags = new BasicDBObject();
            for (String key : _tags.keySet())
                tags.put(key, _tags.get(key));

            return tags;
        }

        @Override
        public boolean isSlaveOk() {
            return _pref.isSlaveOk();
        }

        @Override
        ReplicaSetNode getNode(ReplicaSetStatus.ReplicaSet set) {
            return _pref.getNode(set);
        }

        @Override
        public DBObject toDBObject() {
            return _pref.toDBObject();
        }

        @Override
        public String getName() {
            return _pref.getName();
        }

        private static List<DBObject> splitMapIntoMultipleMaps(DBObject tags) {
            List<DBObject> tagList = new ArrayList<DBObject>(tags.keySet().size());

            for (String key : tags.keySet()) {
                tagList.add(new BasicDBObject(key, tags.get(key).toString()));
            }
            return tagList;
        }

        private DBObject[] getRemainingMaps(final List<DBObject> maps) {
            if (maps.size() <= 1) {
                return new DBObject[0];
            }
            return maps.subList(1, maps.size() - 1).toArray(new DBObject[maps.size() - 1]);
        }

        private final DBObject _tags;
        private final ReadPreference _pref;
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
    public static TaggableReadPreference primaryPreferred(DBObject firstTagSet, DBObject... remainingTagSets) {
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
    public static TaggableReadPreference secondary(DBObject firstTagSet, DBObject... remainingTagSets) {
        return new TaggableReadPreference.SecondaryReadPreference(firstTagSet, remainingTagSets);
    }

    /**
     * @return ReadPreference which reads secondary if available, otherwise from primary.
     */
    public static ReadPreference secondaryPreferred() {
        return SECONDARY_PREFERRED;
    }

    /**
     * @return ReadPreference which reads secondary if available respective of tags, otherwise from primary irrespective of tags.
     */
    public static TaggableReadPreference secondaryPreferred(DBObject firstTagSet, DBObject... remainingTagSets) {
        return new TaggableReadPreference.SecondaryPreferredReadPreference(firstTagSet, remainingTagSets);
    }

    /**
     * @return ReadPreference which reads nearest node.
     */
    public static ReadPreference nearest() {
        return NEAREST;
    }

    public static ReadPreference valueOf(String name) {
        if (name == null) {
            throw new IllegalArgumentException();
        }

        name = name.toLowerCase();

        if (name.equals(PRIMARY.getName().toLowerCase())) {
            return PRIMARY;
        }
        if (name.equals(SECONDARY.getName().toLowerCase())) {
            return SECONDARY;
        }
        if (name.equals(SECONDARY_PREFERRED.getName().toLowerCase())) {
            return SECONDARY_PREFERRED;
        }
        if (name.equals(PRIMARY_PREFERRED.getName().toLowerCase())) {
            return PRIMARY_PREFERRED;
        }
        if (name.equals(NEAREST.getName().toLowerCase())) {
            return NEAREST;
        }

        throw new IllegalArgumentException("No match for read preference of " + name);
    }

    public static TaggableReadPreference valueOf(String name, DBObject firstTagSet, final DBObject... remainingTagSets) {
        if (name == null) {
            throw new IllegalArgumentException();
        }

        name = name.toLowerCase();

        if (name.equals(SECONDARY.getName().toLowerCase())) {
            return new TaggableReadPreference.SecondaryReadPreference(firstTagSet, remainingTagSets);
        }
        if (name.equals(SECONDARY_PREFERRED.getName().toLowerCase())) {
            return new TaggableReadPreference.SecondaryPreferredReadPreference(firstTagSet, remainingTagSets);
        }
        if (name.equals(PRIMARY_PREFERRED.getName().toLowerCase())) {
            return new TaggableReadPreference.PrimaryPreferredReadPreference(firstTagSet, remainingTagSets);
        }
        if (name.equals(NEAREST.getName().toLowerCase())) {
            return new TaggableReadPreference.NearestReadPreference(firstTagSet, remainingTagSets);
        }

        throw new IllegalArgumentException("No match for read preference of " + name);
    }




    /**
     * @return ReadPreference which reads nearest node respective of tags.
     */
    public static TaggableReadPreference nearest(DBObject firstTagSet, DBObject... remainingTagSets) {
        return new TaggableReadPreference.NearestReadPreference(firstTagSet, remainingTagSets);
    }

    public static ReadPreference PRIMARY = new PrimaryReadPreference();
    public static ReadPreference SECONDARY = new TaggableReadPreference.SecondaryReadPreference();

    /**
     * @deprecated As of release 2.9, replaced by
     * <code>ReadPreference.secondaryPreferred(DBObject firstTagSet, DBObject... remainingTagSets)</code>
     */
    @Deprecated
    public static ReadPreference withTags(Map<String, String> tags) {
        return new TaggedReadPreference( tags );
    }

    /**
     * @deprecated As of release 2.9, replaced by
     * <code>ReadPreference.secondaryPreferred(DBObject firstTagSet, DBObject... remainingTagSets)</code>
     */
    @Deprecated
    public static ReadPreference withTags( final DBObject tags ) {
        return new TaggedReadPreference( tags );
    }

    private static final ReadPreference SECONDARY_PREFERRED = new TaggableReadPreference.SecondaryPreferredReadPreference();
    private static final ReadPreference PRIMARY_PREFERRED = new TaggableReadPreference.PrimaryPreferredReadPreference();
    private static final ReadPreference NEAREST = new TaggableReadPreference.NearestReadPreference();
}
