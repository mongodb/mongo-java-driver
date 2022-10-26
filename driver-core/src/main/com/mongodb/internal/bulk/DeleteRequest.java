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
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A representation of a delete.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class DeleteRequest extends WriteRequest {
    private final BsonDocument filter;
    private boolean isMulti = true;
    private Collation collation;
    private Bson hint;
    private String hintString;

    public DeleteRequest(final BsonDocument filter) {
        super();
        this.filter = notNull("filter", filter);
    }

    public BsonDocument getFilter() {
        return filter;
    }

    public DeleteRequest multi(final boolean isMulti) {
        this.isMulti = isMulti;
        return this;
    }

    public boolean isMulti() {
        return isMulti;
    }

    public Collation getCollation() {
        return collation;
    }

    public DeleteRequest collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    public Bson getHint() {
        return hint;
    }

    public DeleteRequest hint(final Bson hint) {
        this.hint = hint;
        return this;
    }

    public String getHintString() {
        return hintString;
    }

    public DeleteRequest hintString(final String hint) {
        this.hintString = hint;
        return this;
    }

    @Override
    public Type getType() {
        return Type.DELETE;
    }
}
