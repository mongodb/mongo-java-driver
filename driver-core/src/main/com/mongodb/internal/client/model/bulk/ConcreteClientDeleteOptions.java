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
import com.mongodb.client.model.bulk.ClientDeleteOptions;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

import java.util.Optional;

import static java.util.Optional.ofNullable;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public final class ConcreteClientDeleteOptions implements ClientDeleteOptions {
    static final ConcreteClientDeleteOptions MUTABLE_EMPTY = new ConcreteClientDeleteOptions();

    @Nullable
    private Collation collation;
    @Nullable
    private Bson hint;
    @Nullable
    private String hintString;

    public ConcreteClientDeleteOptions() {
    }

    @Override
    public ClientDeleteOptions collation(@Nullable final Collation collation) {
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
    public ClientDeleteOptions hint(@Nullable final Bson hint) {
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
    public ClientDeleteOptions hintString(@Nullable final String hintString) {
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
    public String toString() {
        return "ClientDeleteOptions{"
                + "collation=" + collation
                + ", hint=" + hint
                + ", hintString='" + hintString + '\''
                + '}';
    }
}