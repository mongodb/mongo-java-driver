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

import com.mongodb.client.result.bulk.ClientInsertOneResult;
import com.mongodb.lang.Nullable;
import org.bson.BsonValue;

import java.util.Objects;
import java.util.Optional;

import static java.util.Optional.ofNullable;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public final class ConcreteClientInsertOneResult implements ClientInsertOneResult {
    @Nullable
    private final BsonValue insertedId;

    public ConcreteClientInsertOneResult(@Nullable final BsonValue insertedId) {
        this.insertedId = insertedId;
    }

    @Override
    public Optional<BsonValue> getInsertedId() {
        return ofNullable(insertedId);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ConcreteClientInsertOneResult that = (ConcreteClientInsertOneResult) o;
        return Objects.equals(insertedId, that.insertedId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(insertedId);
    }

    @Override
    public String toString() {
        return "ClientInsertOneResult{"
                + "insertedId=" + insertedId
                + '}';
    }
}
