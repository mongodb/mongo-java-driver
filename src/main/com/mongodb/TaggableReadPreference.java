package com.mongodb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Abstract base class for all preference which can be combined with tags
 *
 * @author breinero
 */
public abstract class TaggableReadPreference extends ReadPreference {
    private final static List<DBObject> EMPTY = new ArrayList<DBObject>();

    TaggableReadPreference() {
        _tags = EMPTY;
    }

    TaggableReadPreference(DBObject firstTagSet, DBObject... remainingTagSets) {
        if (firstTagSet == null) {
            throw new IllegalArgumentException("Must have at least one tag set");
        }
        _tags = new ArrayList<DBObject>();
        _tags.add(firstTagSet);
        Collections.addAll(_tags, remainingTagSets);
    }

    @Override
    public boolean isSlaveOk() {
        return true;
    }

    @Override
    public DBObject toDBObject() {
        DBObject readPrefObject = new BasicDBObject("mode", getName());

        if (!_tags.isEmpty())
            readPrefObject.put("tags", _tags);

        return readPrefObject;
    }


    public List<DBObject> getTagSets() {
        List<DBObject> tags = new ArrayList<DBObject>();
        for (DBObject tagSet : _tags) {
            tags.add(tagSet);
        }
        return tags;
    }

    @Override
    public String toString() {
        return getName() + printTags();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final TaggableReadPreference that = (TaggableReadPreference) o;

        if (!_tags.equals(that._tags)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = _tags.hashCode();
        result = 31 * result + getName().hashCode();
        return result;
    }

    String printTags() {
        return (_tags.isEmpty() ? "" :  " : " + new BasicDBObject("tags", _tags));
    }

    private static List<ReplicaSetStatus.Tag> getTagListFromDBObject(final DBObject curTagSet) {
        List<ReplicaSetStatus.Tag> tagList = new ArrayList<ReplicaSetStatus.Tag>();
        for (String key : curTagSet.keySet()) {
            tagList.add(new ReplicaSetStatus.Tag(key, curTagSet.get(key).toString()));
        }
        return tagList;
    }

    final List<DBObject> _tags;

    /**
     * Read from secondary
     *
     * @author breinero
     */
    static class SecondaryReadPreference extends TaggableReadPreference {
        SecondaryReadPreference() {
        }

        SecondaryReadPreference(DBObject firstTagSet, DBObject... remainingTagSets) {
            super(firstTagSet, remainingTagSets);
        }

        @Override
        public String getName() {
            return "secondary";
        }

        @Override
        ReplicaSetStatus.ReplicaSetNode getNode(ReplicaSetStatus.ReplicaSet set) {

            if (_tags.isEmpty())
                return set.getASecondary();

            for (DBObject curTagSet : _tags) {
                List<ReplicaSetStatus.Tag> tagList = getTagListFromDBObject(curTagSet);
                ReplicaSetStatus.ReplicaSetNode node = set.getASecondary(tagList);
                if (node != null) {
                    return node;
                }
            }
            return null;
        }

    }

    /**
     * Read from secondary if available, otherwise from primary, irrespective of tags.
     *
     * @author breinero
     */
    static class SecondaryPreferredReadPreference extends SecondaryReadPreference {
        SecondaryPreferredReadPreference() {
        }

        SecondaryPreferredReadPreference(DBObject firstTagSet, DBObject... remainingTagSets) {
            super(firstTagSet, remainingTagSets);
        }

        @Override
        public String getName() {
            return "secondaryPreferred";
        }

        @Override
        ReplicaSetStatus.ReplicaSetNode getNode(ReplicaSetStatus.ReplicaSet set) {
            ReplicaSetStatus.ReplicaSetNode node = super.getNode(set);
            return (node != null) ? node : set.getMaster();
        }
    }

    /**
     * Read from nearest node respective of tags.
     *
     * @author breinero
     */
    static class NearestReadPreference extends TaggableReadPreference {
        NearestReadPreference() {
        }

        NearestReadPreference(DBObject firstTagSet, DBObject... remainingTagSets) {
            super(firstTagSet, remainingTagSets);
        }


        @Override
        public String getName() {
            return "nearest";
        }


        @Override
        ReplicaSetStatus.ReplicaSetNode getNode(ReplicaSetStatus.ReplicaSet set) {

            if (_tags.isEmpty())
                return set.getAMember();

            for (DBObject curTagSet : _tags) {
                List<ReplicaSetStatus.Tag> tagList = getTagListFromDBObject(curTagSet);
                ReplicaSetStatus.ReplicaSetNode node = set.getAMember(tagList);
                if (node != null) {
                    return node;
                }
            }
            return null;
        }
    }

    /**
     * Read from primary if available, otherwise a secondary.
     *
     * @author breinero
     */
    static class PrimaryPreferredReadPreference extends SecondaryReadPreference {
        PrimaryPreferredReadPreference() {}

        PrimaryPreferredReadPreference(DBObject firstTagSet, DBObject... remainingTagSets) {
            super(firstTagSet, remainingTagSets);
        }

        @Override
        public String getName() {
            return "primaryPreferred";
        }

        @Override
        ReplicaSetStatus.ReplicaSetNode getNode(ReplicaSetStatus.ReplicaSet set) {
            ReplicaSetStatus.ReplicaSetNode node = set.getMaster();
            return (node != null) ? node : super.getNode(set);
        }
    }




}
