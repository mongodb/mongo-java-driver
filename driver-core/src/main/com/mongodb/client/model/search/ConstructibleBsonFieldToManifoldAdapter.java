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

import com.mongodb.internal.client.model.AbstractConstructibleBsonField;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.conversions.Bson;

import static org.bson.assertions.Assertions.notNull;

final class ConstructibleBsonFieldToManifoldAdapter extends AbstractConstructibleBsonField<ConstructibleBsonFieldToManifoldAdapter>
        implements
        ExistsSearchOperator,
        FacetSearchCollector,
        StringSearchFacet, NumericSearchFacet, DateSearchFacet {
    ConstructibleBsonFieldToManifoldAdapter(final String name, final Bson value) {
        super(name, value);
    }

    @Override
    public StringSearchFacet numBuckets(final int max) {
        return newWithAppendedValue("numBuckets", new BsonInt32(max));
    }

    @Override
    public ConstructibleBsonFieldToManifoldAdapter defaultBucket(final String name) {
        return newWithAppendedValue("default", new BsonString(notNull("name", name)));
    }

    @Override
    protected ConstructibleBsonFieldToManifoldAdapter newSelf(final String name, final Bson value) {
        return new ConstructibleBsonFieldToManifoldAdapter(name, value);
    }
}
