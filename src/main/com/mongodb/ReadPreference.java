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

import com.mongodb.ReplicaSetStatus.Node;
import com.mongodb.ReplicaSetStatus.Tag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * an abstract class that represents prefered replica set members to which a query or command can be sent
 */
public abstract class ReadPreference {

    private ReadPreference() {
    }

    /**
     * @return <code>true</code> if this preference allows reads or commands from secondary nodes
     */
    public abstract boolean isSlaveOk();

    abstract Node getNode(ReplicaSetStatus.ReplicaSet set);

    /**
     * @return <code>DBObject</code> representation of this preference
     */
    public abstract DBObject toDBObject();

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
            return "ReadPreference.PRIMARY";
        }

        @Override
        Node getNode(ReplicaSetStatus.ReplicaSet set) {
            return set.getMaster();
        }

        @Override
        public DBObject toDBObject() {
            return new BasicDBObject("mode", "primary");
        }
    }

    /**
     * Abstract base class for all preference which can be combined with tags
     *
     * @author breinero
     */
    private static abstract class TaggableReadPreference extends ReadPreference {
        protected TaggableReadPreference() {
            _tags = null;
        }

        protected TaggableReadPreference(DBObject... tagSetList) {
            _tags = new ArrayList<DBObject>();
            Collections.addAll(_tags, tagSetList);
        }

        public List<DBObject> getTagSets() {
            if (_tags == null)
                return null;

            List<DBObject> tags = new ArrayList<DBObject>();
            for (DBObject tagSet : _tags) {
                tags.add(tagSet);
            }
            return tags;
        }

        protected String printTags() {
            return ((_tags != null && !_tags.isEmpty()) ? " : " + (new BasicDBObject("tags", _tags)).toString() : "");
        }

        protected final List<DBObject> _tags;
    }

    /**
     * Read from primary if available, otherwise a secondary.
     *
     * @author breinero
     */
    private static class PrimaryPreferredReadPreference extends SecondaryReadPreference {
        private PrimaryPreferredReadPreference(DBObject... tagSetList) {
            super(tagSetList);
        }

        public DBObject toDBObject() {
            DBObject readPrefObject = new BasicDBObject("mode", "primaryPreferred");

            if (_tags != null)
                readPrefObject.put("tags", _tags);

            return readPrefObject;
        }

        @Override
        public boolean isSlaveOk() {
            return true;
        }

        @Override
        public String toString() {
            return "ReadPreference.PRIMARY_PREFERRED" + printTags();
        }

        @Override
        Node getNode(ReplicaSetStatus.ReplicaSet set) {
            Node node = set.getMaster();
            return (node != null) ? node : super.getNode(set);
        }
    }

    /**
     * Read from secondary
     *
     * @author breinero
     */
    private static class SecondaryReadPreference extends TaggableReadPreference {
        private SecondaryReadPreference() {
        }

        private SecondaryReadPreference(DBObject... tagSetList) {
            super(tagSetList);
        }

        @Override
        public DBObject toDBObject() {
            DBObject readPrefObject = new BasicDBObject("mode", "secondary");

            if (_tags != null)
                readPrefObject.put("tags", _tags);

            return readPrefObject;
        }

        @Override
        public boolean isSlaveOk() {
            return true;
        }

        @Override
        public String toString() {
            return "ReadPreference.SECONDARY" + printTags();
        }

        @Override
        Node getNode(ReplicaSetStatus.ReplicaSet set) {

            if (_tags == null || _tags.isEmpty())
                return set.getASecondary();

            for (DBObject curTagSet : _tags) {
                List<Tag> tagList = new ArrayList<Tag>();
                for (String key : curTagSet.keySet()) {
                    tagList.add(new Tag(key, curTagSet.get(key).toString()));
                }
                Node node = set.getASecondary(tagList);
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
    private static class SecondaryPreferredReadPreference extends SecondaryReadPreference {
        private SecondaryPreferredReadPreference(DBObject... tagSetList) {
            super(tagSetList);
        }

        @Override
        public String toString() {
            return "ReadPreference.SECONDARY_PREFERRED" + printTags();
        }

        public DBObject toDBObject() {
            DBObject readPrefObject = new BasicDBObject("mode", "secondaryPreferred");

            if (_tags != null)
                readPrefObject.put("tags", _tags);

            return readPrefObject;
        }

        @Override
        Node getNode(ReplicaSetStatus.ReplicaSet set) {
            Node node = super.getNode(set);
            return (node != null) ? node : set.getMaster();
        }
    }

    /**
     * Read from nearest node respective of tags.
     *
     * @author breinero
     */
    private static class NearestReadPreference extends TaggableReadPreference {
        private NearestReadPreference(DBObject... tagSetList) {
            super(tagSetList);
        }

        @Override
        public boolean isSlaveOk() {
            return true;
        }

        @Override
        public String toString() {
            return "ReadPreference.NEAREST" + printTags();
        }

        @Override
        public DBObject toDBObject() {
            DBObject readPrefObject = new BasicDBObject("mode", "nearest");

            if (_tags != null)
                readPrefObject.put("tags", _tags);

            return readPrefObject;
        }

        @Override
        Node getNode(ReplicaSetStatus.ReplicaSet set) {

            if (_tags == null || _tags.isEmpty())
                return set.getAMember();

            for (DBObject curTagSet : _tags) {
                List<Tag> tagList = new ArrayList<Tag>();
                for (String key : curTagSet.keySet()) {
                    tagList.add(new Tag(key, curTagSet.get(key).toString()));
                }
                Node node = set.getAMember(tagList);
                if (node != null) {
                    return node;
                }
            }
            return null;
        }
    }

    /**
     * Read from a secondary if available and matches tags, otherwise read from the primary.
     *
     * @deprecated As of release 2.9, replaced by <code>ReadPeference.secondaryPreferred(DBObject... tagSetList)</code>
     */
    @Deprecated
    public static class TaggedReadPreference extends ReadPreference {

        public TaggedReadPreference(Map<String, String> tags) {
            if (tags == null) {
                throw new IllegalArgumentException("tags can not be null");
            }
            _tags = new BasicDBObject(tags);
        }

        public TaggedReadPreference(DBObject tags) {
            if (tags == null) {
                throw new IllegalArgumentException("tags can not be null");
            }
            _tags = tags;
        }

        private static DBObject[] splitMapIntoMultipleMaps(DBObject tags) {
            DBObject[] tagList = new DBObject[tags.keySet().size()];

            int i = 0;
            for (String key : tags.keySet()) {
                tagList[i] = new BasicDBObject(key, tags.get(key).toString());
                i++;
            }
            return tagList;
        }

        public DBObject getTags() {
            if (_tags == null)
                return null;

            DBObject tags = new BasicDBObject();
            for (String key : _tags.keySet())
                tags.put(key, _tags.get(key));

            return tags;
        }

        @Override
        public boolean isSlaveOk() {
            return true;
        }

        @Override
        Node getNode(ReplicaSetStatus.ReplicaSet set) {
            ReadPreference pref = new SecondaryReadPreference(splitMapIntoMultipleMaps(_tags));
            return pref.getNode(set);
        }

        @Override
        public DBObject toDBObject() {
            ReadPreference pref = new SecondaryReadPreference(splitMapIntoMultipleMaps(_tags));
            return pref.toDBObject();
        }

        private final DBObject _tags;
    }

    /**
     * @return ReadPreference which reads from primary only
     */
    public static ReadPreference primary() {
        return new PrimaryReadPreference();
    }

    /**
     * @return ReadPreference which reads primary if available, otherwise a secondary respective of tags.
     */
    public static ReadPreference primaryPreferred(DBObject... tagSetList) {
        return new PrimaryPreferredReadPreference(tagSetList);
    }

    /**
     * @return ReadPreference which reads secondary repsective of tags.
     */
    public static ReadPreference secondary(DBObject... tagSetList) {
        return new SecondaryReadPreference(tagSetList);
    }

    /**
     * @return ReadPreference which reads secondary if available, otherwise from primary irrespective of tags.
     */
    public static ReadPreference secondaryPreferred(DBObject... tagSetList) {
        return new SecondaryPreferredReadPreference(tagSetList);
    }

    /**
     * @return ReadPreference which reads nearest node repsective of tags.
     */
    public static ReadPreference nearest(DBObject... tagSetList) {
        return new NearestReadPreference(tagSetList);
    }

    public static ReadPreference PRIMARY = primary();
    public static ReadPreference SECONDARY = secondary();

}
