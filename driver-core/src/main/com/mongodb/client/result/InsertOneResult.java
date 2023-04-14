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

import com.mongodb.lang.Nullable;
import org.bson.BsonValue;

import java.util.Objects;

/**
 * The result of an insert one operation.  If the insert one was unacknowledged, then {@code wasAcknowledged} will
 * return false and all other methods will throw {@code UnsupportedOperationException}.
 *
 * @see com.mongodb.WriteConcern#UNACKNOWLEDGED
 * @since 4.0
 */
public abstract class InsertOneResult {

    /**
     * Returns true if the write was acknowledged.
     *
     * @return true if the write was acknowledged
     */
    public abstract boolean wasAcknowledged();

    /**
     * If the _id of the inserted document is available, otherwise null
     *
     * <p>Note: Inserting RawBsonDocuments does not generate an _id value.</p>
     *
     * @return if _id of the inserted document is available, otherwise null
     */
    @Nullable
    public abstract BsonValue getInsertedId();

    /**
     * Create an acknowledged InsertOneResult
     *
     * @param insertId      the id of the inserted document
     * @return an acknowledged InsertOneResult
     */
    public static InsertOneResult acknowledged(@Nullable final BsonValue insertId) {
        return new AcknowledgedInsertOneResult(insertId);
    }

    /**
     * Create an unacknowledged InsertOneResult
     *
     * @return an unacknowledged InsertOneResult
     */
    public static InsertOneResult unacknowledged() {
        return new UnacknowledgedInsertOneResult();
    }

    private static class AcknowledgedInsertOneResult extends InsertOneResult {
        private final BsonValue insertedId;

        AcknowledgedInsertOneResult(@Nullable final BsonValue insertId) {
            this.insertedId = insertId;
        }

        @Override
        public boolean wasAcknowledged() {
            return true;
        }

        @Override
        @Nullable
        public BsonValue getInsertedId() {
            return insertedId;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AcknowledgedInsertOneResult that = (AcknowledgedInsertOneResult) o;
            return Objects.equals(insertedId, that.insertedId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(insertedId);
        }

        @Override
        public String toString() {
            return "AcknowledgedInsertOneResult{"
                    + "insertedId=" + insertedId
                    + '}';
        }
    }

    private static class UnacknowledgedInsertOneResult extends InsertOneResult {
        @Override
        public boolean wasAcknowledged() {
            return false;
        }

        @Override
        @Nullable
        public BsonValue getInsertedId() {
            throw new UnsupportedOperationException("Cannot get information about an unacknowledged insert");
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
            return "UnacknowledgedInsertOneResult{}";
        }
    }
}
