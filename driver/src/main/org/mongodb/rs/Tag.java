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

package org.mongodb.rs;

import org.bson.types.Document;
import org.mongodb.annotations.Immutable;

/**
 * Simple class to hold a single tag, both key and value
 * <p/>
 * NOT PART OF PUBLIC API YET
 */
@Immutable
public final class Tag {
    private final String key;
    private final String value;

    public Tag(final String key, final String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Tag tag = (Tag) o;

        if (key != null ? !key.equals(tag.key) : tag.key != null) {
            return false;
        }
        if (value != null ? !value.equals(tag.value) : tag.value != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    public Document toDBObject() {
        return new Document(key, value);
    }
}
