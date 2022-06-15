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
package com.mongodb.client.model.fill;

import com.mongodb.internal.client.model.AbstractConstructibleBson;
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.client.model.Util.sizeAtLeast;

final class FillConstructibleBson extends AbstractConstructibleBson<FillConstructibleBson> implements FillOptions {
    static final FillConstructibleBson EMPTY_IMMUTABLE = new FillConstructibleBson(AbstractConstructibleBson.EMPTY_IMMUTABLE);

    FillConstructibleBson(final Bson base) {
        super(base);
    }

    private FillConstructibleBson(final Bson base, final Document appended) {
        super(base, appended);
    }

    @Override
    protected FillConstructibleBson newSelf(final Bson base, final Document appended) {
        return new FillConstructibleBson(base, appended);
    }

    @Override
    public <TExpression> FillOptions partitionBy(final TExpression partitionBy) {
        return newMutated(doc -> {
            doc.remove("partitionByFields");
            doc.append("partitionBy", notNull("partitionBy", partitionBy));
        });
    }

    @Override
    public FillOptions partitionByFields(final Iterable<String> partitionByFields) {
        return newMutated(doc -> {
            doc.remove("partitionBy");
            if (sizeAtLeast(partitionByFields, 1)) {
                doc.append("partitionByFields", notNull("partitionByFields", partitionByFields));
            } else {
                doc.remove("partitionByFields");
            }
        });
    }

    @Override
    public FillOptions sortBy(final Bson sortBy) {
        return newAppended("sortBy", notNull("sortBy", sortBy));
    }

    @Override
    public FillOptions option(final String name, final Object value) {
        return newAppended(notNull("name", name), notNull("value", value));
    }
}
