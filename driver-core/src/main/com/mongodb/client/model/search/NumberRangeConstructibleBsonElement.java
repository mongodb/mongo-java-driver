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

final class NumberRangeConstructibleBsonElement extends RangeConstructibleBsonElement<Number, NumberRangeConstructibleBsonElement>
        implements NumberRangeSearchOperator {
    NumberRangeConstructibleBsonElement(final String name, final Bson value) {
        super(name, value);
    }

    @Override
    protected NumberRangeConstructibleBsonElement newSelf(final String name, final Bson value) {
        return new NumberRangeConstructibleBsonElement(name, value);
    }

    @Override
    public NumberRangeSearchOperator gt(final Number l) {
        return internalGt(l);
    }

    @Override
    public NumberRangeSearchOperator lt(final Number u) {
        return internalLt(u);
    }

    @Override
    public NumberRangeSearchOperator gte(final Number l) {
        return internalGte(l);
    }

    @Override
    public NumberRangeSearchOperator lte(final Number u) {
        return internalLte(u);
    }

    @Override
    public NumberRangeSearchOperator gtLt(final Number l, final Number u) {
        return internalGtLt(l, u);
    }

    @Override
    public NumberRangeSearchOperator gteLte(final Number l, final Number u) {
        return internalGteLte(l, u);
    }

    @Override
    public NumberRangeSearchOperator gtLte(final Number l, final Number u) {
        return internalGtLte(l, u);
    }

    @Override
    public NumberRangeSearchOperator gteLt(final Number l, final Number u) {
        return internalGteLt(l, u);
    }
}
