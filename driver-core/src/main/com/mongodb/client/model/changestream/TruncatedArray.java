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

package com.mongodb.client.model.changestream;

import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.util.Objects;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * A part of an {@link UpdateDescription} object specifying a change to a field of the {@linkplain org.bson.BsonType#ARRAY array} type
 * when the change is reported as truncation.
 *
 * @since 4.3
 */
public final class TruncatedArray {
    private final String field;
    private final int newSize;

    /**
     * @param field   The name of the field that was truncated.
     * @param newSize The size of the new field value.
     */
    @BsonCreator
    public TruncatedArray(@BsonProperty("field") final String field, @BsonProperty("newSize") final int newSize) {
        this.field = notNull("field", field);
        isTrueArgument("newSize >= 0", newSize >= 0);
        this.newSize = newSize;
    }

    /**
     * Returns the name of the truncated field.
     *
     * @return {@code field}.
     */
    public String getField() {
        return field;
    }

    /**
     * Returns the size of the new {@linkplain #getField() field} value.
     *
     * @return {@code newSize}.
     */
    public int getNewSize() {
        return newSize;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TruncatedArray that = (TruncatedArray) o;
        return newSize == that.newSize && field.equals(that.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, newSize);
    }

    @Override
    public String toString() {
        return "TruncatedArray{"
                + "field=" + field
                + ", newSize=" + newSize
                + '}';
    }
}
