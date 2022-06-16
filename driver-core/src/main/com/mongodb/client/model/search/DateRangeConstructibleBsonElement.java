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

    private DateRangeConstructibleBsonElement(final Bson baseElement, final Bson appendedElementValue) {
        super(baseElement, appendedElementValue);
    }

    @Override
    protected DateRangeConstructibleBsonElement newSelf(final Bson baseElement, final Bson appendedElementValue) {
        return new DateRangeConstructibleBsonElement(baseElement, appendedElementValue);
    }

    @Override
    public DateRangeSearchOperator gt(final Instant l) {
        return internalGt(l);
    }

    @Override
    public DateRangeSearchOperator lt(final Instant u) {
        return internalLt(u);
    }

    @Override
    public DateRangeSearchOperator gte(final Instant l) {
        return internalGte(l);
    }

    @Override
    public DateRangeSearchOperator lte(final Instant u) {
        return internalLte(u);
    }

    @Override
    public DateRangeSearchOperator gtLt(final Instant l, final Instant u) {
        return internalGtLt(l, u);
    }

    @Override
    public DateRangeSearchOperator gteLte(final Instant l, final Instant u) {
        return internalGteLte(l, u);
    }

    @Override
    public DateRangeSearchOperator gtLte(final Instant l, final Instant u) {
        return internalGtLte(l, u);
    }

    @Override
    public DateRangeSearchOperator gteLt(final Instant l, final Instant u) {
        return internalGteLt(l, u);
    }
}
