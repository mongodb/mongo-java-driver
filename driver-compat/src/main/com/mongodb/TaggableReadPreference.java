/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

import org.bson.types.Document;
import org.mongodb.annotations.Immutable;

import java.util.ArrayList;
import java.util.List;

@Immutable
public class TaggableReadPreference extends ReadPreference {
    private final org.mongodb.TaggableReadPreference proxiedTaggable;

    TaggableReadPreference(final org.mongodb.TaggableReadPreference proxied) {
        super(proxied);
        this.proxiedTaggable = proxied;
    }

    public List<DBObject> getTagSets() {
        final List<DBObject> tagSets = new ArrayList<DBObject>();
        for (final Document cur : proxiedTaggable.getTagSets()) {
            tagSets.add(DBObjects.toDBObject(cur));
        }
        return tagSets;
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
