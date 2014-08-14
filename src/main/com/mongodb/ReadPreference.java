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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;


/**
 * An abstract class that represents preferred replica set members to which a query or command can be sent.
 *
 * @mongodb.driver.manual applications/replication/#replica-set-read-preference  Read Preference
 */
public abstract class ReadPreference {

    ReadPreference() {
    }

    /**
     * @return <code>true</code> if this preference allows reads or commands from secondary nodes
     */
    public abstract boolean isSlaveOk();

    /**
     * @return <code>DBObject</code> representation of this preference
     * @deprecated for internal use only
     */
    @Deprecated
    public abstract DBObject toDBObject();

    /**
     * The name of this read preference.
     *
     * @return the name
     */
    public abstract String getName();

    abstract List<ServerDescription> choose(final ClusterDescription clusterDescription);

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
        List<ServerDescription> choose(final ClusterDescription clusterDescription) {
            return clusterDescription.getPrimaries();
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
            _pref = new TaggableReadPreference.SecondaryReadPreference(toTagsList(maps.get(0), getRemainingMaps(maps)));

        }

        public TaggedReadPreference(DBObject tags) {
            if (tags == null || tags.keySet().size() == 0) {
                throw new IllegalArgumentException("tags can not be null or empty");
            }
            _tags = tags;
            List<DBObject> maps = splitMapIntoMultipleMaps(_tags);
            _pref = new TaggableReadPreference.SecondaryReadPreference(toTagsList(maps.get(0), getRemainingMaps(maps)));
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
        List<ServerDescription> choose(final ClusterDescription clusterDescription) {
            return _pref.choose(clusterDescription);
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
     * Gets a read preference that forces read to the primary.
     *
     * @return ReadPreference which reads from primary only
     */
    public static ReadPreference primary() {
        return _PRIMARY;
    }

    /**
     * Gets a read preference that forces reads to the primary if available, otherwise to a secondary.
     *
     * @return ReadPreference which reads primary if available.
     */
    public static ReadPreference primaryPreferred() {
        return _PRIMARY_PREFERRED;
    }

    /**
     * Gets a read preference that forces reads to a secondary.
     *
     * @return ReadPreference which reads secondary.
     */
    public static ReadPreference secondary() {
        return _SECONDARY;
    }

    /**
     * Gets a read preference that forces reads to a secondary if one is available, otherwise to the primary.
     *
     * @return ReadPreference which reads secondary if available, otherwise from primary.
     */
    public static ReadPreference secondaryPreferred() {
        return _SECONDARY_PREFERRED;
    }

    /**
     * Gets a read preference that forces reads to a primary or a secondary.
     *
     * @return ReadPreference which reads nearest
     */
    public static ReadPreference nearest() {
        return _NEAREST;
    }

    /**
     * Gets a read preference that forces reads to the primary if available, otherwise to a secondary with the given set of tags.
     *
     * @param tagSet the set of tags to limit the list of secondaries to.
     * @return ReadPreference which reads primary if available, otherwise a secondary respective of tags.\
     * @since 2.13
     */
    public static TaggableReadPreference primaryPreferred(TagSet tagSet) {
        return primaryPreferred(asList(tagSet));
    }

    /**
     * Gets a read preference that forces reads to a secondary with the given set of tags.
     *
     * @param tagSet the set of tags to limit the list of secondaries to
     * @return ReadPreference which reads secondary respective of tags.
     * @since 2.13
     */
    public static TaggableReadPreference secondary(TagSet tagSet) {
        return secondary(asList(tagSet));
    }

    /**
     * Gets a read preference that forces reads to a secondary with the given set of tags, or the primary is none are available.
     *
     * @param tagSet the set of tags to limit the list of secondaries to
     * @return ReadPreference which reads secondary if available respective of tags, otherwise from primary irrespective of tags.
     * @since 2.13
     */
    public static TaggableReadPreference secondaryPreferred(TagSet tagSet) {
        return secondaryPreferred(asList(tagSet));
    }

    /**
     * Gets a read preference that forces reads to the primary or a secondary with the given set of tags.
     *
     * @param tagSet the set of tags to limit the list of secondaries to
     * @return ReadPreference which reads nearest node respective of tags.
     * @since 2.13
     */
    public static TaggableReadPreference nearest(TagSet tagSet) {
        return nearest(asList(tagSet));
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
    public static TaggableReadPreference primaryPreferred(List<TagSet> tagSetList) {
        return new TaggableReadPreference.PrimaryPreferredReadPreference(tagSetList);
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
    public static TaggableReadPreference secondary(List<TagSet> tagSetList) {
        return new TaggableReadPreference.SecondaryReadPreference(tagSetList);
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
    public static TaggableReadPreference secondaryPreferred(List<TagSet> tagSetList) {
        return new TaggableReadPreference.SecondaryPreferredReadPreference(tagSetList);
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
    public static TaggableReadPreference nearest(List<TagSet> tagSetList) {
        return new TaggableReadPreference.NearestReadPreference(tagSetList);
    }

    /**
     * @return ReadPreference which reads primary if available, otherwise a secondary respective of tags.
     * @deprecated use factory methods that take {@code TagSet} instead
     * @see com.mongodb.ReadPreference#primaryPreferred(TagSet)
     * @see com.mongodb.ReadPreference#primaryPreferred(java.util.List)
     */
    @Deprecated
    public static TaggableReadPreference primaryPreferred(DBObject firstTagSet, DBObject... remainingTagSets) {
        return new TaggableReadPreference.PrimaryPreferredReadPreference(toTagsList(firstTagSet, remainingTagSets));
    }

    /**
     * @return ReadPreference which reads secondary respective of tags.
     * @deprecated use factory methods that take {@code TagSet} instead
     * @see com.mongodb.ReadPreference#secondary(TagSet)
     * @see com.mongodb.ReadPreference#secondary(java.util.List)
     */
    @Deprecated
    public static TaggableReadPreference secondary(DBObject firstTagSet, DBObject... remainingTagSets) {
        return new TaggableReadPreference.SecondaryReadPreference(toTagsList(firstTagSet, remainingTagSets));
    }

    /**
     * @return ReadPreference which reads secondary if available respective of tags, otherwise from primary irrespective of tags.
     * @deprecated use factory methods that take {@code TagSet} instead
     * @see com.mongodb.ReadPreference#secondaryPreferred(TagSet)
     * @see com.mongodb.ReadPreference#secondaryPreferred(java.util.List)
     */
    @Deprecated
    public static TaggableReadPreference secondaryPreferred(DBObject firstTagSet, DBObject... remainingTagSets) {
        return new TaggableReadPreference.SecondaryPreferredReadPreference(toTagsList(firstTagSet, remainingTagSets));
    }

    /**
     * @return ReadPreference which reads nearest node respective of tags.
     * @deprecated use factory methods that take {@code TagSet} instead
     * @see com.mongodb.ReadPreference#nearest(TagSet)
     * @see com.mongodb.ReadPreference#nearest(java.util.List)
     */
    @Deprecated
    public static TaggableReadPreference nearest(DBObject firstTagSet, DBObject... remainingTagSets) {
        return new TaggableReadPreference.NearestReadPreference(toTagsList(firstTagSet, remainingTagSets));
    }

    /**
     * Creates a read preference from the given read preference name.
     *
     * @param name the name of the read preference
     * @return the read preference
     */
    public static ReadPreference valueOf(String name) {
        if (name == null) {
            throw new IllegalArgumentException();
        }

        name = name.toLowerCase();

        if (name.equals(_PRIMARY.getName().toLowerCase())) {
            return _PRIMARY;
        }
        if (name.equals(_SECONDARY.getName().toLowerCase())) {
            return _SECONDARY;
        }
        if (name.equals(_SECONDARY_PREFERRED.getName().toLowerCase())) {
            return _SECONDARY_PREFERRED;
        }
        if (name.equals(_PRIMARY_PREFERRED.getName().toLowerCase())) {
            return _PRIMARY_PREFERRED;
        }
        if (name.equals(_NEAREST.getName().toLowerCase())) {
            return _NEAREST;
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
    public static TaggableReadPreference valueOf(String name, List<TagSet> tagSetList) {
        if (name == null) {
            throw new IllegalArgumentException();
        }

        name = name.toLowerCase();

        if (name.equals(PRIMARY.getName().toLowerCase())) {
            throw new IllegalArgumentException("Primary read preference can not also specify tag sets");
        }

        if (name.equals(_SECONDARY.getName().toLowerCase())) {
            return secondary(tagSetList);
        }
        if (name.equals(_SECONDARY_PREFERRED.getName().toLowerCase())) {
            return secondaryPreferred(tagSetList);
        }
        if (name.equals(_PRIMARY_PREFERRED.getName().toLowerCase())) {
            return primaryPreferred(tagSetList);
        }
        if (name.equals(_NEAREST.getName().toLowerCase())) {
            return nearest(tagSetList);
        }

        throw new IllegalArgumentException("No match for read preference of " + name);
    }

    /**
     * Creates a taggable read preference from the given read preference name and list of tag sets.
     *
     * @param name the name of the read preference
     * @param firstTagSet the first set of tags
     * @param remainingTagSets the remaining set of tags
     * @return the taggable read preference
     * @deprecated use method that takes a {@code List<TagSet>}
     */
    @Deprecated
    public static TaggableReadPreference valueOf(String name, DBObject firstTagSet, final DBObject... remainingTagSets) {
        return valueOf(name, toTagsList(firstTagSet, remainingTagSets));
    }

    /**
     * A primary read preference.  Equivalent to calling {@code ReadPreference.primary()}.
     *
     * @see com.mongodb.ReadPreference#primary()
     * @deprecated As of release 2.9.0, replaced by {@code ReadPreference.primary()}
     */
    @Deprecated
    public static final ReadPreference PRIMARY;

    /**
     * A secondary-preferred read preference.  Equivalent to calling
     * {@code ReadPreference.secondaryPreferred}.  This reference should really have been called
     * {@code ReadPreference.SECONDARY_PREFERRED}, but the naming of it preceded the idea of distinguishing
     * between secondary and secondary-preferred, so for backwards compatibility, leaving the name as is with
     * the behavior as it was when it was created.
     *
     * @see com.mongodb.ReadPreference#secondary()
     * @see com.mongodb.ReadPreference#secondaryPreferred()
     * @deprecated As of release 2.9.0, replaced by {@code ReadPreference.secondaryPreferred()}
     */
    @Deprecated
    public static final ReadPreference SECONDARY;

    /**
     * @deprecated As of release 2.9.0, replaced by
     * {@code ReadPreference.secondaryPreferred(DBObject firstTagSet, DBObject... remainingTagSets)}
     */
    @Deprecated
    public static ReadPreference withTags(Map<String, String> tags) {
        return new TaggedReadPreference( tags );
    }

    /**
     * @deprecated As of release 2.9.0, replaced by
     * {@code ReadPreference.secondaryPreferred(DBObject firstTagSet, DBObject... remainingTagSets)}
     */
    @Deprecated
    public static ReadPreference withTags( final DBObject tags ) {
        return new TaggedReadPreference( tags );
    }

    private static final ReadPreference _PRIMARY;
    private static final ReadPreference _SECONDARY;
    private static final ReadPreference _SECONDARY_PREFERRED;
    private static final ReadPreference _PRIMARY_PREFERRED;
    private static final ReadPreference _NEAREST;

    private static List<TagSet> toTagsList(DBObject firstTagSet, DBObject... remainingTagSets) {
        List<TagSet> tagsList = new ArrayList<TagSet>(remainingTagSets.length + 1);
        tagsList.add(toTags(firstTagSet));
        for (DBObject cur : remainingTagSets) {
            tagsList.add(toTags(cur));
        }

        return tagsList;
    }

    private static TagSet toTags(final DBObject tagsDocument) {
        List<Tag> tagList = new ArrayList<Tag>();
        for (String key : tagsDocument.keySet()) {
            tagList.add(new Tag(key, tagsDocument.get(key).toString()));
        }
        return new TagSet(tagList);
    }

    static {
        _PRIMARY = new PrimaryReadPreference();
        _SECONDARY = new TaggableReadPreference.SecondaryReadPreference();
        _SECONDARY_PREFERRED = new TaggableReadPreference.SecondaryPreferredReadPreference();
        _PRIMARY_PREFERRED = new TaggableReadPreference.PrimaryPreferredReadPreference();
        _NEAREST = new TaggableReadPreference.NearestReadPreference();

         PRIMARY = _PRIMARY;
         SECONDARY = _SECONDARY_PREFERRED;  // this is not a bug.  See SECONDARY Javadoc.
    }
}
