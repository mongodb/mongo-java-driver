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

/**
 * Represents an upsert request in a bulk write operation that resulted in an insert. It contains the index of the upsert request in the
 * operation and the value of the _id field of the inserted document.
 *
 * @since 2.12
 * @see BulkWriteRequestBuilder#upsert()
 *
 * @mongodb.server.release 2.6
 * @mongodb.driver.manual reference/command/update/#update.upserted Bulk Upsert
 */
public class BulkWriteUpsert {
    private final int index;
    private final Object id;

    /**
     * Constructs an instance.
     *
     * @param index the index of the item that was upserted
     * @param id the value of the _id of the upserted item
     */
    public BulkWriteUpsert(final int index, final Object id) {
        this.index = index;
        this.id = id;
    }

    /**
     * Gets the index of the upserted item based on the order it was added to the bulk write operation.
     *
     * @return the index
     */
    public int getIndex() {
        return index;
    }

    /**
     * Gets the id of the upserted item.
     *
     * @return the id
     */
    public Object getId() {
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

        final BulkWriteUpsert that = (BulkWriteUpsert) o;

        if (index != that.index) {
            return false;
        }
        if (!id.equals(that.id)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = index;
        result = 31 * result + id.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "BulkWriteUpsert{"
               + "index=" + index
               + ", id=" + id
               + '}';
    }
}
