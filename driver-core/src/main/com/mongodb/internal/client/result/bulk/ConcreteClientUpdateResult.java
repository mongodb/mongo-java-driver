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
package com.mongodb.internal.client.result.bulk;

import com.mongodb.client.model.bulk.ClientUpdateResult;
import com.mongodb.lang.Nullable;
import org.bson.BsonValue;

import java.util.Objects;
import java.util.Optional;

import static java.util.Optional.ofNullable;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public final class ConcreteClientUpdateResult implements ClientUpdateResult {
    private final long matchedCount;
    private final long modifiedCount;
    @Nullable
    private final BsonValue upsertedId;

    public ConcreteClientUpdateResult(
            final long matchedCount,
            final long modifiedCount,
            @Nullable final BsonValue upsertedId) {
        this.matchedCount = matchedCount;
        this.modifiedCount = modifiedCount;
        this.upsertedId = upsertedId;
    }

    @Override
    public long getMatchedCount() {
        return matchedCount;
    }

    @Override
    public long getModifiedCount() {
        return modifiedCount;
    }

    @Override
    public Optional<BsonValue> getUpsertedId() {
        return ofNullable(upsertedId);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ConcreteClientUpdateResult that = (ConcreteClientUpdateResult) o;
        return matchedCount == that.matchedCount
                && modifiedCount == that.modifiedCount
                && Objects.equals(upsertedId, that.upsertedId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(matchedCount, modifiedCount, upsertedId);
    }

    @Override
    public String toString() {
        return "ClientUpdateResult{"
                + "matchedCount=" + matchedCount
                + ", modifiedCount=" + modifiedCount
                + ", upsertedId=" + upsertedId
                + '}';
    }
}
