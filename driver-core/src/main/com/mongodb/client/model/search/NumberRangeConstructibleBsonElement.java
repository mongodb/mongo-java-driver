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
    public NumberRangeConstructibleBsonElement gt(final Number l) {
        return super.gt(l);
    }

    @Override
    public NumberRangeConstructibleBsonElement lt(final Number u) {
        return super.lt(u);
    }

    @Override
    public NumberRangeConstructibleBsonElement gte(final Number l) {
        return super.gte(l);
    }

    @Override
    public NumberRangeConstructibleBsonElement lte(final Number u) {
        return super.lte(u);
    }

    @Override
    public NumberRangeConstructibleBsonElement gtLt(final Number l, final Number u) {
        return super.gtLt(l, u);
    }

    @Override
    public NumberRangeConstructibleBsonElement gteLte(final Number l, final Number u) {
        return super.gteLte(l, u);
    }

    @Override
    public NumberRangeConstructibleBsonElement gtLte(final Number l, final Number u) {
        return super.gtLte(l, u);
    }

    @Override
    public NumberRangeConstructibleBsonElement gteLt(final Number l, final Number u) {
        return super.gteLt(l, u);
    }
}
