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
package com.mongodb.client.model.densify;

import com.mongodb.internal.client.model.AbstractConstructibleBson;
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.client.model.Util.sizeAtLeast;

final class DensifyConstructibleBson extends AbstractConstructibleBson<DensifyConstructibleBson> implements
        NumberDensifyRange, DateDensifyRange,
        DensifyOptions {
    static final DensifyConstructibleBson EMPTY_IMMUTABLE = new DensifyConstructibleBson(AbstractConstructibleBson.EMPTY_IMMUTABLE);

    DensifyConstructibleBson(final Bson base) {
        super(base);
    }

    private DensifyConstructibleBson(final Bson base, final Document appended) {
        super(base, appended);
    }

    @Override
    protected DensifyConstructibleBson newSelf(final Bson base, final Document appended) {
        return new DensifyConstructibleBson(base, appended);
    }

    @Override
    public DensifyOptions partitionByFields(final Iterable<String> fields) {
        notNull("partitionByFields", fields);
        return newMutated(doc -> {
            if (sizeAtLeast(fields, 1)) {
                doc.append("partitionByFields", fields);
            } else {
                doc.remove("partitionByFields");
            }
        });
    }

    @Override
    public DensifyOptions option(final String name, final Object value) {
        return newAppended(notNull("name", name), notNull("value", value));
    }
}
