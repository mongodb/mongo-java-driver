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
package com.mongodb.client.model.search;

import com.mongodb.annotations.Immutable;
import com.mongodb.internal.client.model.AbstractConstructibleBson;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

final class VectorSearchNestedConstructibleBson extends AbstractConstructibleBson<VectorSearchNestedConstructibleBson>
        implements VectorSearchNestedOptions {
    /**
     * An {@linkplain Immutable immutable} {@link BsonDocument#isEmpty() empty} instance.
     */
    static final VectorSearchNestedConstructibleBson EMPTY_IMMUTABLE =
            new VectorSearchNestedConstructibleBson(AbstractConstructibleBson.EMPTY_IMMUTABLE);

    VectorSearchNestedConstructibleBson(final Bson base) {
        super(base);
    }

    private VectorSearchNestedConstructibleBson(final Bson base, final Document appended) {
        super(base, appended);
    }

    @Override
    protected VectorSearchNestedConstructibleBson newSelf(final Bson base, final Document appended) {
        return new VectorSearchNestedConstructibleBson(base, appended);
    }

    @Override
    public VectorSearchNestedOptions scoreMode(final VectorSearchScoreMode scoreMode) {
        return newAppended("scoreMode", notNull("scoreMode", scoreMode).getValue());
    }

    @Override
    public VectorSearchNestedOptions option(final String name, final Object value) {
        return newAppended(notNull("name", name), notNull("value", value));
    }
}
