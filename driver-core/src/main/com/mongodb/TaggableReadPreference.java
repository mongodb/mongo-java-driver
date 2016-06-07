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
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Abstract class for all preference which can be combined with tags
 */
@Immutable
public abstract class TaggableReadPreference extends ReadPreference {
    private final List<TagSet> tagSetList = new ArrayList<TagSet>();

    TaggableReadPreference() {
    }

    TaggableReadPreference(final TagSet tagSet) {
        tagSetList.add(tagSet);
    }

    TaggableReadPreference(final List<TagSet> tagSetList) {
        notNull("tagSetList", tagSetList);

        for (final TagSet tagSet : tagSetList) {
            this.tagSetList.add(tagSet);
        }
    }

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

    @Override
    public String toString() {
        return getName() + (tagSetList.isEmpty() ? "" : ": " + tagSetList);
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

        return tagSetList.equals(that.tagSetList);
    }

    @Override
    public int hashCode() {
        int result = tagSetList.hashCode();
        result = 31 * result + getName().hashCode();
        return result;
    }

    /**
     * Read from secondary
     */
    static class SecondaryReadPreference extends TaggableReadPreference {
        SecondaryReadPreference() {
        }

        SecondaryReadPreference(final TagSet tagSet) {
            super(tagSet);
        }

        SecondaryReadPreference(final List<TagSet> tagSetList) {
            super(tagSetList);
        }

        @Override
        public String getName() {
            return "secondary";
        }

        @Override
        @SuppressWarnings("deprecation")
        public List<ServerDescription> choose(final ClusterDescription clusterDescription) {

            if (getTagSetList().isEmpty()) {
                return clusterDescription.getSecondaries();
            }

            for (final TagSet tagSet : getTagSetList()) {
                List<ServerDescription> servers = clusterDescription.getSecondaries(tagSet);
                if (!servers.isEmpty()) {
                    return servers;
                }
            }
            return Collections.emptyList();
        }

    }

    /**
     * Read from secondary if available, otherwise from primary, irrespective of tags.
     */
    static class SecondaryPreferredReadPreference extends SecondaryReadPreference {
        SecondaryPreferredReadPreference() {
        }

        SecondaryPreferredReadPreference(final TagSet tagSet) {
            super(tagSet);
        }

        SecondaryPreferredReadPreference(final List<TagSet> tagSetList) {
            super(tagSetList);
        }

        @Override
        public String getName() {
            return "secondaryPreferred";
        }

        @Override
        @SuppressWarnings("deprecation")
        public List<ServerDescription> choose(final ClusterDescription clusterDescription) {
            List<ServerDescription> servers = super.choose(clusterDescription);
            return (!servers.isEmpty()) ? servers : clusterDescription.getPrimaries();
        }
    }

    /**
     * Read from nearest node respective of tags.
     */
    static class NearestReadPreference extends TaggableReadPreference {
        NearestReadPreference() {
        }

        NearestReadPreference(final TagSet tagSet) {
            super(tagSet);
        }

        NearestReadPreference(final List<TagSet> tagSetList) {
            super(tagSetList);
        }


        @Override
        public String getName() {
            return "nearest";
        }


        @Override
        @SuppressWarnings("deprecation")
        public List<ServerDescription> choose(final ClusterDescription clusterDescription) {

            if (getTagSetList().isEmpty()) {
                return clusterDescription.getAnyPrimaryOrSecondary();
            }

            for (final TagSet tagSet : getTagSetList()) {
                List<ServerDescription> servers = clusterDescription.getAnyPrimaryOrSecondary(tagSet);
                if (!servers.isEmpty()) {
                    return servers;
                }
            }
            return Collections.emptyList();
        }
    }

    /**
     * Read from primary if available, otherwise a secondary.
     */
    static class PrimaryPreferredReadPreference extends SecondaryReadPreference {
        PrimaryPreferredReadPreference() {
        }

        PrimaryPreferredReadPreference(final TagSet tagSet) {
            super(tagSet);
        }

        PrimaryPreferredReadPreference(final List<TagSet> tagSetList) {
            super(tagSetList);
        }

        @Override
        public String getName() {
            return "primaryPreferred";
        }

        @Override
        @SuppressWarnings("deprecation")
        public List<ServerDescription> choose(final ClusterDescription clusterDescription) {
            List<ServerDescription> servers = clusterDescription.getPrimaries();
            return (!servers.isEmpty()) ? servers : super.choose(clusterDescription);
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
