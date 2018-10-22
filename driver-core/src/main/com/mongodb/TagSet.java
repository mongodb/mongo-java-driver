/*
 * Copyright 2008-present MongoDB, Inc.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An immutable set of tags, used to select members of a replica set to use for read operations.
 *
 * @mongodb.driver.manual tutorial/configure-replica-set-tag-sets Tag Sets
 * @see com.mongodb.ReadPreference
 * @since 2.13
 */
@Immutable
public final class TagSet implements Iterable<Tag> {
    private final List<Tag> wrapped;

    /**
     * An empty set of tags.
     */
    public TagSet() {
        wrapped = Collections.emptyList();
    }

    /**
     * A set of tags contain the single given tag
     *
     * @param tag the tag
     */
    public TagSet(final Tag tag) {
        notNull("tag", tag);
        wrapped = Collections.singletonList(tag);
    }

    /**
     * A set of tags containing the given list of tags.
     *
     * @param tagList the list of tags
     */
    public TagSet(final List<Tag> tagList) {
        notNull("tagList", tagList);

        // Ensure no duplicates
        Set<String> tagNames = new HashSet<String>();
        for (Tag tag : tagList) {
            if (tag == null) {
                throw new IllegalArgumentException("Null tags are not allowed");
            }
            if (!tagNames.add(tag.getName())) {
                throw new IllegalArgumentException("Duplicate tag names not allowed in a tag set: " + tag.getName());
            }
        }
        this.wrapped = Collections.unmodifiableList(new ArrayList<Tag>(tagList));
    }

    @Override
    public Iterator<Tag> iterator() {
        return wrapped.iterator();
    }

    /**
     * Returns {@code true} if this tag set contains all of the elements of the specified tag set.
     *
     * @param tagSet tag set to be checked for containment in this tag set
     * @return {@code true} if this tag set contains all of the elements of the specified tag set
     */
    public boolean containsAll(final TagSet tagSet) {
        return wrapped.containsAll(tagSet.wrapped);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final TagSet tags = (TagSet) o;

        if (!wrapped.equals(tags.wrapped)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return wrapped.hashCode();
    }

    @Override
    public String toString() {
        return "TagSet{"
               + wrapped
               + '}';
    }
}
