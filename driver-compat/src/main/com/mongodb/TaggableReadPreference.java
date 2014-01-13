/*
 * Copyright (c) 2008 MongoDB, Inc.
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

import org.mongodb.annotations.Immutable;
import org.mongodb.connection.Tags;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Immutable
public class TaggableReadPreference extends ReadPreference {
    private final org.mongodb.TaggableReadPreference proxiedTaggable;

    TaggableReadPreference(final org.mongodb.TaggableReadPreference proxied) {
        super(proxied);
        this.proxiedTaggable = proxied;
    }

    public List<DBObject> getTagSets() {
        List<DBObject> documents = new ArrayList<DBObject>(proxiedTaggable.getTagsList().size());
        for (final Tags tag : proxiedTaggable.getTagsList()) {
            documents.add(new BasicDBObject(tag));
        }
        return Collections.unmodifiableList(documents);
    }

    @Override
    public DBObject toDBObject() {
        DBObject document = new BasicDBObject("mode", getName());

        if (!proxiedTaggable.getTagsList().isEmpty()) {
            document.put("tags", getTagSets());
        }

        return document;
    }

    @Override
    public String toString() {
        return proxiedTaggable.toString();
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

        if (!proxiedTaggable.equals(that.proxiedTaggable)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return proxiedTaggable.hashCode();
    }
}
