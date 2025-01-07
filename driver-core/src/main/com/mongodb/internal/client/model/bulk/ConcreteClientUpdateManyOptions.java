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

import java.util.Optional;

import static java.util.Optional.ofNullable;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public final class ConcreteClientUpdateManyOptions implements ClientUpdateManyOptions {
    static final ConcreteClientUpdateManyOptions MUTABLE_EMPTY = new ConcreteClientUpdateManyOptions();

    @Nullable
    private Iterable<? extends Bson> arrayFilters;
    @Nullable
    private Collation collation;
    @Nullable
    private Bson hint;
    @Nullable
    private String hintString;
    @Nullable
    private Boolean upsert;

    public ConcreteClientUpdateManyOptions() {
    }

    @Override
    public ClientUpdateManyOptions arrayFilters(@Nullable final Iterable<? extends Bson> arrayFilters) {
        this.arrayFilters = arrayFilters;
        return this;
    }

    /**
     * @see #arrayFilters(Iterable)
     */
    public Optional<Iterable<? extends Bson>> getArrayFilters() {
        return ofNullable(arrayFilters);
    }

    @Override
    public ClientUpdateManyOptions collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * @see #collation(Collation)
     */
    public Optional<Collation> getCollation() {
        return ofNullable(collation);
    }

    @Override
    public ClientUpdateManyOptions hint(@Nullable final Bson hint) {
        this.hint = hint;
        this.hintString = null;
        return this;
    }

    /**
     * @see #hint(Bson)
     */
    public Optional<Bson> getHint() {
        return ofNullable(hint);
    }

    @Override
    public ClientUpdateManyOptions hintString(@Nullable final String hintString) {
        this.hintString = hintString;
        this.hint = null;
        return this;
    }

    /**
     * @see #hintString(String)
     */
    public Optional<String> getHintString() {
        return ofNullable(hintString);
    }

    @Override
    public ClientUpdateManyOptions upsert(@Nullable final Boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    /**
     * @see #isUpsert()
     */
    public Optional<Boolean> isUpsert() {
        return ofNullable(upsert);
    }

    @Override
    public String toString() {
        return "ClientUpdateManyOptions{"
                + "arrayFilters=" + arrayFilters
                + ", collation=" + collation
                + ", hint=" + hint
                + ", hintString='" + hintString + '\''
                + ", upsert=" + upsert
                + '}';
    }
}
