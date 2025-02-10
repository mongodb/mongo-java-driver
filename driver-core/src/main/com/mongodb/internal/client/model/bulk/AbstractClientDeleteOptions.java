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
import com.mongodb.client.model.bulk.ClientDeleteManyOptions;
import com.mongodb.client.model.bulk.ClientDeleteOneOptions;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

import java.util.Optional;

import static java.util.Optional.ofNullable;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public abstract class AbstractClientDeleteOptions {
    @Nullable
    private Collation collation;
    @Nullable
    private Bson hint;
    @Nullable
    private String hintString;

    AbstractClientDeleteOptions() {
    }

    public AbstractClientDeleteOptions collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * @see ClientDeleteOneOptions#collation(Collation)
     * @see ClientDeleteManyOptions#collation(Collation)
     */
    public Optional<Collation> getCollation() {
        return ofNullable(collation);
    }

    public AbstractClientDeleteOptions hint(@Nullable final Bson hint) {
        this.hint = hint;
        this.hintString = null;
        return this;
    }

    /**
     * @see ClientDeleteOneOptions#hint(Bson)
     * @see ClientDeleteManyOptions#hint(Bson)
     */
    public Optional<Bson> getHint() {
        return ofNullable(hint);
    }

    public AbstractClientDeleteOptions hintString(@Nullable final String hintString) {
        this.hintString = hintString;
        this.hint = null;
        return this;
    }

    /**
     * @see ClientDeleteOneOptions#hintString(String)
     * @see ClientDeleteManyOptions#hintString(String)
     */
    public Optional<String> getHintString() {
        return ofNullable(hintString);
    }
}
