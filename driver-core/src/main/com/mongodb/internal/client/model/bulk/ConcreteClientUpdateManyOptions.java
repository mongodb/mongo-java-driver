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
import com.mongodb.client.model.bulk.ClientUpdateManyOptions;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public final class ConcreteClientUpdateManyOptions extends AbstractClientUpdateOptions implements ClientUpdateManyOptions {
    static final ConcreteClientUpdateManyOptions MUTABLE_EMPTY = new ConcreteClientUpdateManyOptions();

    public ConcreteClientUpdateManyOptions() {
    }

    @Override
    public ConcreteClientUpdateManyOptions arrayFilters(@Nullable final Iterable<? extends Bson> arrayFilters) {
        return (ConcreteClientUpdateManyOptions) super.arrayFilters(arrayFilters);
    }

    @Override
    public ConcreteClientUpdateManyOptions collation(@Nullable final Collation collation) {
        return (ConcreteClientUpdateManyOptions) super.collation(collation);
    }

    @Override
    public ConcreteClientUpdateManyOptions hint(@Nullable final Bson hint) {
        return (ConcreteClientUpdateManyOptions) super.hint(hint);
    }

    @Override
    public ConcreteClientUpdateManyOptions hintString(@Nullable final String hintString) {
        return (ConcreteClientUpdateManyOptions) super.hintString(hintString);
    }

    @Override
    public ConcreteClientUpdateManyOptions upsert(@Nullable final Boolean upsert) {
        return (ConcreteClientUpdateManyOptions) super.upsert(upsert);
    }

    @Override
    public String toString() {
        return "ClientUpdateManyOptions{"
                + "arrayFilters=" + getArrayFilters().orElse(null)
                + ", collation=" + getCollation().orElse(null)
                + ", hint=" + getHint().orElse(null)
                + ", hintString=" + getHintString().map(s -> '\'' + s + '\'') .orElse(null)
                + ", upsert=" + isUpsert().orElse(null)
                + '}';
    }
}
