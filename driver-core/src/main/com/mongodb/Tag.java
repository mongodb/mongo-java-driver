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

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A replica set tag.
 *
 * @since 2.13
 * @mongodb.driver.manual tutorial/configure-replica-set-tag-sets Tag Sets
 */
@Immutable
public final class Tag {
    private final String name;
    private final String value;

    /**
     * Construct a new instance.
     *
     * @param name the tag name
     * @param value the value of the tag
     */
    public Tag(final String name, final String value) {
        this.name = notNull("name", name);
        this.value = notNull("value", value);
    }

    /**
     * Gets the name of the replica set tag.
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the value of the replica set tag.
     * @return the value
     */
    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Tag that = (Tag) o;

        if (!name.equals(that.name)) {
            return false;
        }
        if (!value.equals(that.value)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Tag{"
               + "name='" + name + '\''
               + ", value='" + value + '\''
               + '}';
    }
}
