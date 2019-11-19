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

package com.mongodb.client.result;

import org.bson.BsonValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableMap;

/**
 * The result of an insert many operation.  If the insert many was unacknowledged, then {@code wasAcknowledged} will
 * return false and all other methods will throw {@code UnsupportedOperationException}.
 *
 * @see com.mongodb.WriteConcern#UNACKNOWLEDGED
 * @since 4.0
 */
public abstract class InsertManyResult {

    /**
     * Returns true if the write was acknowledged.
     *
     * @return true if the write was acknowledged
     */
    public abstract boolean wasAcknowledged();

    /**
     * An unmodifiable map of the index of the inserted document to the id of the inserted document.
     *
     * <p>Note: Inserting RawBsonDocuments does not generate an _id value and it's corresponding value will be null.</p>
     *
     * @return  A map of the index of the inserted document to the id of the inserted document.
     */
    public abstract Map<Integer, BsonValue> getInsertedIds();

    /**
     * Create an acknowledged InsertManyResult
     *
     * @param insertedIds the map of the index of the inserted document to the id of the inserted document.
     * @return an acknowledged InsertManyResult
     */
    public static InsertManyResult acknowledged(final Map<Integer, BsonValue> insertedIds) {
        return new AcknowledgedInsertManyResult(insertedIds);
    }

    /**
     * Create an unacknowledged InsertManyResult
     *
     * @return an unacknowledged InsertManyResult
     */
    public static InsertManyResult unacknowledged() {
        return new UnacknowledgedInsertManyResult();
    }

    private static class AcknowledgedInsertManyResult extends InsertManyResult {
        private final Map<Integer, BsonValue> insertedIds;

        AcknowledgedInsertManyResult(final Map<Integer, BsonValue> insertedIds) {
            this.insertedIds = unmodifiableMap(new HashMap<>(insertedIds));
        }

        @Override
        public boolean wasAcknowledged() {
            return true;
        }

        @Override
        public Map<Integer, BsonValue> getInsertedIds() {
            return insertedIds;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AcknowledgedInsertManyResult that = (AcknowledgedInsertManyResult) o;
            return Objects.equals(insertedIds, that.insertedIds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(insertedIds);
        }

        @Override
        public String toString() {
            return "AcknowledgedInsertManyResult{"
                    + "insertedIds=" + insertedIds
                    + '}';
        }
    }

    private static class UnacknowledgedInsertManyResult extends InsertManyResult {
        @Override
        public boolean wasAcknowledged() {
            return false;
        }

        @Override
        public Map<Integer, BsonValue> getInsertedIds() {
            throw  new UnsupportedOperationException("Cannot get information about an unacknowledged insert many");
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public String toString() {
            return "UnacknowledgedInsertManyResult{}";
        }
    }
}
