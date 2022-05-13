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

import org.bson.conversions.Bson;

import java.time.Instant;

final class DateRangeConstructibleBsonElement extends RangeConstructibleBsonElement<Instant, DateRangeConstructibleBsonElement>
        implements DateRangeSearchOperator {
    DateRangeConstructibleBsonElement(final String name, final Bson value) {
        super(name, value);
    }

    @Override
    protected DateRangeConstructibleBsonElement newSelf(final String name, final Bson value) {
        return new DateRangeConstructibleBsonElement(name, value);
    }

    @Override
    public DateRangeConstructibleBsonElement gt(final Instant l) {
        return super.gt(l);
    }

    @Override
    public DateRangeConstructibleBsonElement lt(final Instant u) {
        return super.lt(u);
    }

    @Override
    public DateRangeConstructibleBsonElement gte(final Instant l) {
        return super.gte(l);
    }

    @Override
    public DateRangeConstructibleBsonElement lte(final Instant u) {
        return super.lte(u);
    }

    @Override
    public DateRangeConstructibleBsonElement gtLt(final Instant l, final Instant u) {
        return super.gtLt(l, u);
    }

    @Override
    public DateRangeConstructibleBsonElement gteLte(final Instant l, final Instant u) {
        return super.gteLte(l, u);
    }

    @Override
    public DateRangeConstructibleBsonElement gtLte(final Instant l, final Instant u) {
        return super.gtLte(l, u);
    }

    @Override
    public DateRangeConstructibleBsonElement gteLt(final Instant l, final Instant u) {
        return super.gteLt(l, u);
    }
}
