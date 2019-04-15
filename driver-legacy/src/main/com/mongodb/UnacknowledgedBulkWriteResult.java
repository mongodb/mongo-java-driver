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

import java.util.List;

class UnacknowledgedBulkWriteResult extends BulkWriteResult {

    UnacknowledgedBulkWriteResult() {
    }

    @Override
    public boolean isAcknowledged() {
        return false;
    }

    @Override
    public int getInsertedCount() {
        throw getUnacknowledgedWriteException();
    }

    @Override
    public int getMatchedCount() {
        throw getUnacknowledgedWriteException();
    }

    @Override
    public int getRemovedCount() {
        throw getUnacknowledgedWriteException();
    }

    @Override
    public int getModifiedCount() {
        throw getUnacknowledgedWriteException();
    }

    @Override
    public List<BulkWriteUpsert> getUpserts() {
        throw getUnacknowledgedWriteException();
    }

    private UnsupportedOperationException getUnacknowledgedWriteException() {
        return new UnsupportedOperationException("Can not get information about an unacknowledged write");
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
        return "UnacknowledgedBulkWriteResult{"
               + '}';
    }
}

