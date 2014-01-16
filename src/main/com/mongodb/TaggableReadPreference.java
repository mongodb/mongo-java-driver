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

    @Override
    List<ServerDescription> choose(final ClusterDescription clusterDescription) {
        if (_tags.isEmpty()) {
            return getServers(clusterDescription);
        }
        for (DBObject curTagSet : _tags) {
            Tags tags = getTagsFromDBObject(curTagSet);
            List<ServerDescription> taggedServers = getServersForTags(clusterDescription, tags);
            if (!taggedServers.isEmpty()) {
                return taggedServers;
            }
        }
        return Collections.emptyList();
    }

    abstract List<ServerDescription> getServers(final ClusterDescription clusterDescription);

    abstract List<ServerDescription> getServersForTags(final ClusterDescription clusterDescription, final Tags tags);

    String printTags() {
        return (_tags.isEmpty() ? "" :  " : " + new BasicDBObject("tags", _tags));
    }

    // TODO
    private static Tags getTagsFromDBObject(final DBObject curTagSet) {
        Tags tags = new Tags();
        for (String key : curTagSet.keySet()) {
            tags.append(key, curTagSet.get(key).toString());
        }
        return tags;
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
        List<ServerDescription> getServers(final ClusterDescription clusterDescription) {
            return clusterDescription.getSecondaries();
        }

        @Override
        List<ServerDescription> getServersForTags(final ClusterDescription clusterDescription, final Tags tags) {
            return clusterDescription.getSecondaries(tags);
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
        List<ServerDescription> choose(final ClusterDescription clusterDescription) {
            final List<ServerDescription> servers = super.choose(clusterDescription);
            return (!servers.isEmpty()) ? servers : clusterDescription.getPrimaries();
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
        List<ServerDescription> getServers(final ClusterDescription clusterDescription) {
            return clusterDescription.getAny();
        }

        @Override
        List<ServerDescription> getServersForTags(final ClusterDescription clusterDescription, final Tags tags) {
            return clusterDescription.getAny(tags);
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
        List<ServerDescription> choose(final ClusterDescription clusterDescription) {
            final List<ServerDescription> servers = clusterDescription.getPrimaries();
            return (!servers.isEmpty()) ? servers : super.choose(clusterDescription);
        }
    }
}
