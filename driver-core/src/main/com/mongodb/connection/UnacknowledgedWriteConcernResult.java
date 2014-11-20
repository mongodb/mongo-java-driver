/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.connection;

import com.mongodb.UnacknowledgedWriteException;
import com.mongodb.WriteConcernResult;
import org.bson.BsonValue;

/**
 * This class is not part of the public API.  It may change or be removed at any time.
 */
public class UnacknowledgedWriteConcernResult implements WriteConcernResult {
    @Override
    public boolean wasAcknowledged() {
        return false;
    }

    @Override
    public int getCount() {
        throw new UnacknowledgedWriteException("Cannot get information about an unacknowledged write");
    }

    @Override
    public boolean isUpdateOfExisting() {
        throw new UnacknowledgedWriteException("Cannot get information about an unacknowledged write");
    }

    @Override
    public BsonValue getUpsertedId() {
        throw new UnacknowledgedWriteException("Cannot get information about an unacknowledged write");
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
        return 1;
    }

    @Override
    public String toString() {
        return "UnacknowledgedWriteResult{"
               + '}';
    }
}
