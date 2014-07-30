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

/**
 * Abstract class for all preference which can be combined with tags
 */
@Immutable
public abstract class TaggableReadPreference extends ReadPreference {
    private final List<Tags> tagsList = new ArrayList<Tags>();

    TaggableReadPreference() {
    }

    TaggableReadPreference(final Tags tags) {
        tagsList.add(Tags.freeze(tags));
    }

    TaggableReadPreference(final List<Tags> tagsList) {
        for (final Tags tags : tagsList) {
            this.tagsList.add(Tags.freeze(tags));
        }
    }

    @Override
    public boolean isSlaveOk() {
        return true;
    }

    public BsonDocument toDocument() {
        BsonDocument readPrefObject = new BsonDocument("mode", new BsonString(getName()));

        if (!tagsList.isEmpty()) {
            readPrefObject.put("tags", tagsListToBsonArray());
        }

        return readPrefObject;
    }

    public List<Tags> getTagsList() {
        return Collections.unmodifiableList(tagsList);
    }

    @Override
    public String toString() {
        return getName() + (tagsList.isEmpty() ? "" : ": " + tagsList);
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

        return tagsList.equals(that.tagsList);
    }

    @Override
    public int hashCode() {
        int result = tagsList.hashCode();
        result = 31 * result + getName().hashCode();
        return result;
    }

    /**
     * Read from secondary
     */
    static class SecondaryReadPreference extends TaggableReadPreference {
        SecondaryReadPreference() {
        }

        SecondaryReadPreference(final Tags tags) {
            super(tags);
        }

        SecondaryReadPreference(final List<Tags> tagsList) {
            super(tagsList);
        }

        @Override
        public String getName() {
            return "secondary";
        }

        @Override
        public List<ServerDescription> choose(final ClusterDescription clusterDescription) {

            if (getTagsList().isEmpty()) {
                return clusterDescription.getSecondaries();
            }

            for (final Tags tags : getTagsList()) {
                List<ServerDescription> servers = clusterDescription.getSecondaries(tags);
                if (!servers.isEmpty()) {
                    return servers;
                }
            }
            return Collections.emptyList();
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

        SecondaryPreferredReadPreference(final Tags tags) {
            super(tags);
        }

        SecondaryPreferredReadPreference(final List<Tags> tagsList) {
            super(tagsList);
        }

        @Override
        public String getName() {
            return "secondaryPreferred";
        }

        @Override
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

        NearestReadPreference(final Tags tags) {
            super(tags);
        }

        NearestReadPreference(final List<Tags> tagsList) {
            super(tagsList);
        }


        @Override
        public String getName() {
            return "nearest";
        }


        @Override
        public List<ServerDescription> choose(final ClusterDescription clusterDescription) {

            if (getTagsList().isEmpty()) {
                return clusterDescription.getAnyPrimaryOrSecondary();
            }

            for (final Tags tags : getTagsList()) {
                List<ServerDescription> servers = clusterDescription.getAnyPrimaryOrSecondary(tags);
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

        PrimaryPreferredReadPreference(final Tags tags) {
            super(tags);
        }

        PrimaryPreferredReadPreference(final List<Tags> tagsList) {
            super(tagsList);
        }

        @Override
        public String getName() {
            return "primaryPreferred";
        }

        @Override
        public List<ServerDescription> choose(final ClusterDescription clusterDescription) {
            List<ServerDescription> servers = clusterDescription.getPrimaries();
            return (!servers.isEmpty()) ? servers : super.choose(clusterDescription);
        }
    }

    private BsonArray tagsListToBsonArray() {
        BsonArray bsonArray = new BsonArray();
        for (Tags tags : tagsList) {
            bsonArray.add(tags.toDocument());
        }
        return bsonArray;
    }
}
