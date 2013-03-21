/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import org.mongodb.annotations.Immutable;
import org.mongodb.rs.ReplicaSet;
import org.mongodb.rs.ReplicaSetMember;
import org.mongodb.rs.Tag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Abstract class for all preference which can be combined with tags
 */
@Immutable
public abstract class TaggableReadPreference extends ReadPreference {
    private static final List<Document> EMPTY = new ArrayList<Document>();
    private final List<Document> tags;

    TaggableReadPreference() {
        tags = EMPTY;
    }

    TaggableReadPreference(final Document firstTagSet, final Document... remainingTagSets) {
        if (firstTagSet == null) {
            throw new IllegalArgumentException("Must have at least one tag set");
        }
        tags = new ArrayList<Document>();
        tags.add(firstTagSet);
        Collections.addAll(tags, remainingTagSets);
    }

    @Override
    public boolean isSlaveOk() {
        return true;
    }

    @Override
    public Document toDocument() {
        final Document readPrefObject = new Document("mode", getName());

        if (!tags.isEmpty()) {
            readPrefObject.put("tags", tags);
        }

        return readPrefObject;
    }

    //CHECKSTYLE:OFF
    public List<Document> getTagSets() {
        final List<Document> tags = new ArrayList<Document>();
        for (final Document tagSet : this.tags) {
            tags.add(tagSet);
        }
        return tags;
    }
    //CHECKSTYLE:ON

    @Override
    public String toString() {
        return getName() + printTags();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final TaggableReadPreference that = (TaggableReadPreference) o;

        return tags.equals(that.tags);
    }

    @Override
    public int hashCode() {
        int result = tags.hashCode();
        result = 31 * result + getName().hashCode();
        return result;
    }

    String printTags() {
        return (tags.isEmpty() ? "" : " : " + new Document("tags", tags));
    }

    private static List<Tag> getTagListFromMongoDocument(final Document curTagSet) {
        final List<Tag> tagList = new ArrayList<Tag>();
        for (final Map.Entry<String, Object> entry : curTagSet.entrySet()) {
            tagList.add(new Tag(entry.getKey(), entry.getValue().toString()));
        }
        return tagList;
    }

    List<Document> getTags() {
        return tags;
    }

    /**
     * Read from secondary
     *
     */
    static class SecondaryReadPreference extends TaggableReadPreference {
        SecondaryReadPreference() {
        }

        SecondaryReadPreference(final Document firstTagSet, final Document... remainingTagSets) {
            super(firstTagSet, remainingTagSets);
        }

        @Override
        public String getName() {
            return "secondary";
        }

        @Override
        public ReplicaSetMember chooseReplicaSetMember(final ReplicaSet set) {

            if (getTags().isEmpty()) {
                return set.getASecondary();
            }

            for (final Document curTagSet : getTags()) {
                final List<Tag> tagList = getTagListFromMongoDocument(curTagSet);
                final ReplicaSetMember node = set.getASecondary(tagList);
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

        SecondaryPreferredReadPreference(final Document firstTagSet, final Document... remainingTagSets) {
            super(firstTagSet, remainingTagSets);
        }

        @Override
        public String getName() {
            return "secondaryPreferred";
        }

        @Override
        public ReplicaSetMember chooseReplicaSetMember(final ReplicaSet set) {
            final ReplicaSetMember node = super.chooseReplicaSetMember(set);
            return (node != null) ? node : set.getPrimary();
        }
    }

    /**
     * Read from nearest node respective of tags.
     *
     */
    static class NearestReadPreference extends TaggableReadPreference {
        NearestReadPreference() {
        }

        NearestReadPreference(final Document firstTagSet, final Document... remainingTagSets) {
            super(firstTagSet, remainingTagSets);
        }


        @Override
        public String getName() {
            return "nearest";
        }


        @Override
        public ReplicaSetMember chooseReplicaSetMember(final ReplicaSet set) {

            if (getTags().isEmpty()) {
                return set.getAMember();
            }

            for (final Document curTagSet : getTags()) {
                final List<Tag> tagList = getTagListFromMongoDocument(curTagSet);
                final ReplicaSetMember node = set.getAMember(tagList);
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
     */
    static class PrimaryPreferredReadPreference extends SecondaryReadPreference {
        PrimaryPreferredReadPreference() {
        }

        PrimaryPreferredReadPreference(final Document firstTagSet, final Document... remainingTagSets) {
            super(firstTagSet, remainingTagSets);
        }

        @Override
        public String getName() {
            return "primaryPreferred";
        }

        @Override
        public ReplicaSetMember chooseReplicaSetMember(final ReplicaSet set) {
            final ReplicaSetMember node = set.getPrimary();
            return (node != null) ? node : super.chooseReplicaSetMember(set);
        }
    }
}
