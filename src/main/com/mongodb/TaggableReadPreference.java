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
 * Abstract class for all preference which can be combined with tags
 */
public abstract class TaggableReadPreference extends ReadPreference {
    TaggableReadPreference() {
        tagSetList = Collections.emptyList();
    }

    TaggableReadPreference(List<TagSet> tagSetList) {
        this.tagSetList = Collections.unmodifiableList(new ArrayList<TagSet>(tagSetList));
    }

    @Override
    public boolean isSlaveOk() {
        return true;
    }

    @Deprecated
    @Override
    public DBObject toDBObject() {
        DBObject readPrefObject = new BasicDBObject("mode", getName());

        if (!tagSetList.isEmpty()) {
            List<DBObject> tagSetDocumentList = new ArrayList<DBObject>();
            for (TagSet tagSet : tagSetList) {
                DBObject tagSetDocument = new BasicDBObject();
                for (Tag tag : tagSet) {
                    tagSetDocument.put(tag.getName(), tag.getValue());
                }
                tagSetDocumentList.add(tagSetDocument);
            }
            readPrefObject.put("tags", tagSetDocumentList);
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
        return tagSetList;
    }

    /**
     * Gets the list of tag sets as a list of DBObject, one for each tag set
     *
     * @return the list of tag sets
     * @see TaggableReadPreference#getTagSetList()
     * @deprecated use the {@code getTagSetList} method instead
     */
    @Deprecated
    public List<DBObject> getTagSets() {
        List<DBObject> tags = new ArrayList<DBObject>();
        for (TagSet curTags: tagSetList) {
            BasicDBObject tagsDocument = new BasicDBObject();
            for (Tag curTag : curTags) {
                tagsDocument.put(curTag.getName(), curTag.getValue());
            }
            tags.add(tagsDocument);
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

        TaggableReadPreference that = (TaggableReadPreference) o;

        if (!tagSetList.equals(that.tagSetList)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = tagSetList.hashCode();
        result = 31 * result + getName().hashCode();
        return result;
    }

    @Override
    List<ServerDescription> choose(final ClusterDescription clusterDescription) {
        if (tagSetList.isEmpty()) {
            return getServers(clusterDescription);
        }
        for (TagSet tags : tagSetList) {
            List<ServerDescription> taggedServers = getServersForTags(clusterDescription, tags);
            if (!taggedServers.isEmpty()) {
                return taggedServers;
            }
        }
        return Collections.emptyList();
    }

    abstract List<ServerDescription> getServers(final ClusterDescription clusterDescription);

    abstract List<ServerDescription> getServersForTags(final ClusterDescription clusterDescription, final TagSet tags);

    String printTags() {
        return tagSetList.isEmpty() ? "" :  " : " + tagSetList;
    }

    final List<TagSet> tagSetList;

    /**
     * Read from secondary
     */
    static class SecondaryReadPreference extends TaggableReadPreference {
        SecondaryReadPreference() {
        }

        SecondaryReadPreference(List<TagSet> tagsList) {
            super(tagsList);
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
        List<ServerDescription> getServersForTags(final ClusterDescription clusterDescription, final TagSet tags) {
            return clusterDescription.getSecondaries(tags);
        }
    }

    /**
     * Read from secondary if available, otherwise from primary, irrespective of tags.
     */
    static class SecondaryPreferredReadPreference extends SecondaryReadPreference {
        SecondaryPreferredReadPreference() {
        }

        SecondaryPreferredReadPreference(List<TagSet> tagsList) {
            super(tagsList);
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
     */
    static class NearestReadPreference extends TaggableReadPreference {
        NearestReadPreference() {
        }

        NearestReadPreference(List<TagSet> tagsList) {
            super(tagsList);
        }

        @Override
        public String getName() {
            return "nearest";
        }

        @Override
        List<ServerDescription> getServers(final ClusterDescription clusterDescription) {
            return clusterDescription.getAnyPrimaryOrSecondary();
        }

        @Override
        List<ServerDescription> getServersForTags(final ClusterDescription clusterDescription, final TagSet tags) {
            return clusterDescription.getAnyPrimaryOrSecondary(tags);
        }
    }

    /**
     * Read from primary if available, otherwise a secondary.
     */
    static class PrimaryPreferredReadPreference extends SecondaryReadPreference {
        PrimaryPreferredReadPreference() {}

        PrimaryPreferredReadPreference(List<TagSet> tagsList) {
            super(tagsList);
        }

        @Override
        public String getName() {
            return "primaryPreferred";
        }

        @Override
        List<ServerDescription> choose(final ClusterDescription clusterDescription) {
            List<ServerDescription> servers = clusterDescription.getPrimaries();
            return (!servers.isEmpty()) ? servers : super.choose(clusterDescription);
        }
    }
}
