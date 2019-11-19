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

package com.mongodb.bulk;

import org.bson.BsonValue;

import java.util.Objects;

/**
 * Represents an item in the bulk write that was inserted.
 *
 * @since 4.0
 */
public class BulkWriteInsert {
    private final int index;
    private final BsonValue id;

    /**
     * Construct an instance.
     *
     * @param index the index in the list of bulk write requests that the insert occurred in
     * @param id the id of the document that was inserted as the result of the insert
     */
    public BulkWriteInsert(final int index, final BsonValue id) {
        this.index = index;
        this.id = id;
    }

    /**
     * Gets the index of the inserted item based on the order it was added to the bulk write operation.
     *
     * @return the index
     */
    public int getIndex() {
        return index;
    }

    /**
     * Gets the id of the inserted item.
     *
     * @return the id
     */
    public BsonValue getId() {
        return id;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final BulkWriteInsert that = (BulkWriteInsert) o;
        return index == that.index && Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, id);
    }

    @Override
    public String toString() {
        return "BulkWriteInsert{"
               + "index=" + index
               + ", id=" + id
               + '}';
    }
}
