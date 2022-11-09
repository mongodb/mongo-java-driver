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

package com.mongodb.client.model;

import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

import java.util.Objects;

/**
 * The options to apply to a $push update operator.
 *
 * @mongodb.driver.manual reference/operator/update/push/ $push
 * @see Updates#pushEach(String, java.util.List, PushOptions)
 * @since 3.1
 */
public class PushOptions {
    private Integer position;
    private Integer slice;
    private Integer sort;
    private Bson sortDocument;

    /**
     * Gets the position at which to add the pushed values in the array.
     *
     * @return the position, which may be null
     * @mongodb.driver.manual reference/operator/update/position/ $position
     */
    @Nullable
    public Integer getPosition() {
        return position;
    }

    /**
     * Sets the position at which to add the pushed values in the array.
     *
     * @param position the position
     * @return this
     * @mongodb.driver.manual reference/operator/update/position/ $position
     */
    public PushOptions position(@Nullable final Integer position) {
        this.position = position;
        return this;
    }

    /**
     * Gets the slice value, which is the limit on the number of array elements allowed.
     *
     * @return the slice value representing the limit on the number of array elements allowed
     * @mongodb.driver.manual reference/operator/update/slice/ $slice
     */
    @Nullable
    public Integer getSlice() {
        return slice;
    }

    /**
     * Sets the limit on the number of array elements allowed.
     *
     * @param slice the limit
     * @return this
     * @mongodb.driver.manual reference/operator/update/slice/ $slice
     */
    public PushOptions slice(@Nullable final Integer slice) {
        this.slice = slice;
        return this;
    }

    /**
     * Gets the sort direction for sorting array elements that are not documents.
     *
     * @return the sort direction
     * @mongodb.driver.manual reference/operator/update/sort/ $sort
     * @mongodb.driver.manual reference/operator/update/sort/#sort-array-elements-that-are-not-documents
     */
    @Nullable
    public Integer getSort() {
        return sort;
    }

    /**
     * Sets the sort direction for sorting array elements that are not documents.
     *
     * @param sort the sort direction
     * @return this
     * @throws IllegalStateException if sortDocument property is already set
     * @mongodb.driver.manual reference/operator/update/sort/ $sort
     * @mongodb.driver.manual reference/operator/update/sort/#sort-array-elements-that-are-not-documents
     */
    public PushOptions sort(@Nullable final Integer sort) {
        if (sortDocument != null) {
            throw new IllegalStateException("sort can not be set if sortDocument already is");
        }
        this.sort = sort;
        return this;
    }

    /**
     * Gets the sort direction for sorting array elements that are documents.
     *
     * @return the sort document
     * @mongodb.driver.manual reference/operator/update/sort/ $sort
     */
    @Nullable
    public Bson getSortDocument() {
        return sortDocument;
    }

    /**
     * Sets the sort direction for sorting array elements that are documents.
     *
     * @param sortDocument the sort document
     * @return this
     * @throws IllegalStateException if sort property is already set
     * @mongodb.driver.manual reference/operator/update/sort/ $sort
     */
    public PushOptions sortDocument(@Nullable final Bson sortDocument) {
        if (sort != null) {
            throw new IllegalStateException("sortDocument can not be set if sort already is");
        }
        this.sortDocument = sortDocument;
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PushOptions that = (PushOptions) o;

        if (!Objects.equals(position, that.position)) {
            return false;
        }
        if (!Objects.equals(slice, that.slice)) {
            return false;
        }
        if (!Objects.equals(sort, that.sort)) {
            return false;
        }
        return Objects.equals(sortDocument, that.sortDocument);
    }

    @Override
    public int hashCode() {
        int result = position != null ? position.hashCode() : 0;
        result = 31 * result + (slice != null ? slice.hashCode() : 0);
        result = 31 * result + (sort != null ? sort.hashCode() : 0);
        result = 31 * result + (sortDocument != null ? sortDocument.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Push Options{"
                       + "position=" + position
                       + ", slice=" + slice
                       + ((sort == null) ? "" : ", sort=" + sort)
                       + ((sortDocument == null) ? "" :  ", sortDocument=" + sortDocument)
                       + '}';
    }
}
