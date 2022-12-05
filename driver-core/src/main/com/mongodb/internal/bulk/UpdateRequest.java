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

package com.mongodb.internal.bulk;

import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An update to one or more documents.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class UpdateRequest extends WriteRequest {
    private final BsonValue update;
    private final Type updateType;
    private final BsonDocument filter;
    private boolean isMulti;
    private boolean isUpsert = false;
    private Collation collation;
    private List<BsonDocument> arrayFilters;
    @Nullable private Bson hint;
    @Nullable private String hintString;

    public UpdateRequest(final BsonDocument filter, @Nullable final BsonValue update, final Type updateType) {
        if (updateType != Type.UPDATE && updateType != Type.REPLACE) {
            throw new IllegalArgumentException("Update type must be UPDATE or REPLACE");
        }
        if (update != null && !update.isDocument() && !update.isArray()) {
            throw new IllegalArgumentException("Update operation type must be a document or a pipeline");
        }

        this.filter = notNull("filter", filter);
        this.update = notNull("update", update);
        this.updateType = updateType;
        this.isMulti = updateType == Type.UPDATE;
    }

    @Override
    public Type getType() {
        return updateType;
    }

    public BsonDocument getFilter() {
        return filter;
    }

    public BsonValue getUpdateValue() {
        return update;
    }

    public boolean isMulti() {
        return isMulti;
    }

    public UpdateRequest multi(final boolean isMulti) {
        if (isMulti && updateType == Type.REPLACE) {
            throw new IllegalArgumentException("Replacements can not be multi");
        }
        this.isMulti = isMulti;
        return this;
    }

    public boolean isUpsert() {
        return isUpsert;
    }

    public UpdateRequest upsert(final boolean isUpsert) {
        this.isUpsert = isUpsert;
        return this;
    }

    @Nullable
    public Collation getCollation() {
        return collation;
    }

    public UpdateRequest collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    public UpdateRequest arrayFilters(@Nullable final List<BsonDocument> arrayFilters) {
        this.arrayFilters = arrayFilters;
        return this;
    }

    @Nullable
    public List<BsonDocument> getArrayFilters() {
        return arrayFilters;
    }

    @Nullable
    public Bson getHint() {
        return hint;
    }

    public UpdateRequest hint(@Nullable final Bson hint) {
        this.hint = hint;
        return this;
    }

    @Nullable
    public String getHintString() {
        return hintString;
    }

    public UpdateRequest hintString(@Nullable final String hint) {
        this.hintString = hint;
        return this;
    }
}

