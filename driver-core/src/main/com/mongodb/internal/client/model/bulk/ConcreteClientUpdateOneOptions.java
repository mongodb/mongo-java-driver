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
package com.mongodb.internal.client.model.bulk;

import com.mongodb.client.model.Collation;
import com.mongodb.client.model.bulk.ClientUpdateOneOptions;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

import java.util.Optional;

import static java.util.Optional.ofNullable;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public final class ConcreteClientUpdateOneOptions extends AbstractClientUpdateOptions implements ClientUpdateOneOptions {
    static final ConcreteClientUpdateOneOptions MUTABLE_EMPTY = new ConcreteClientUpdateOneOptions();

    @Nullable
    private Bson sort;

    public ConcreteClientUpdateOneOptions() {
    }

    @Override
    public ConcreteClientUpdateOneOptions arrayFilters(@Nullable final Iterable<? extends Bson> arrayFilters) {
        return (ConcreteClientUpdateOneOptions) super.arrayFilters(arrayFilters);
    }

    @Override
    public ConcreteClientUpdateOneOptions collation(@Nullable final Collation collation) {
        return (ConcreteClientUpdateOneOptions) super.collation(collation);
    }

    @Override
    public ConcreteClientUpdateOneOptions hint(@Nullable final Bson hint) {
        return (ConcreteClientUpdateOneOptions) super.hint(hint);
    }

    @Override
    public ConcreteClientUpdateOneOptions hintString(@Nullable final String hintString) {
        return (ConcreteClientUpdateOneOptions) super.hintString(hintString);
    }

    @Override
    public ConcreteClientUpdateOneOptions upsert(@Nullable final Boolean upsert) {
        return (ConcreteClientUpdateOneOptions) super.upsert(upsert);
    }

    /**
     * @see ClientUpdateOneOptions#sort(Bson)
     */
    public ConcreteClientUpdateOneOptions sort(final Bson sort) {
        this.sort = sort;
        return this;
    }

    /**
     * @see ClientUpdateOneOptions#sort(Bson)
     */
    public Optional<Bson> getSort() {
        return ofNullable(sort);
    }

    @Override
    public String toString() {
        return "ClientUpdateOneOptions{"
                + "arrayFilters=" + getArrayFilters().orElse(null)
                + ", collation=" + getCollation().orElse(null)
                + ", hint=" + getHint().orElse(null)
                + ", hintString=" + getHintString().map(s -> '\'' + s + '\'') .orElse(null)
                + ", upsert=" + isUpsert().orElse(null)
                + ", sort=" + getSort().orElse(null)
                + '}';
    }
}
