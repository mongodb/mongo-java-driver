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

import com.mongodb.internal.client.model.AbstractConstructibleBsonElement;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static org.bson.assertions.Assertions.notNull;

abstract class RangeConstructibleBsonElement<T, S extends RangeConstructibleBsonElement<T, S>> extends AbstractConstructibleBsonElement<S>
        implements RangeSearchOperator<T> {
    RangeConstructibleBsonElement(final String name, final Bson value) {
        super(name, value);
    }

    @Override
    public final S score(final SearchScore modifier) {
        return newWithAppendedValue("score", notNull("modifier", modifier));
    }

    @Override
    public S gt(final T l) {
        return newWithMutatedValue(l, false, null, false);
    }

    @Override
    public S lt(final T u) {
        return newWithMutatedValue(null, false, u, false);
    }

    @Override
    public S gte(final T l) {
        return newWithMutatedValue(l, true, null, false);
    }

    @Override
    public S lte(final T u) {
        return newWithMutatedValue(null, false, u, true);
    }

    @Override
    public S gtLt(final T l, final T u) {
        return newWithMutatedValue(l, false, u, false);
    }

    @Override
    public S gteLte(final T l, final T u) {
        return newWithMutatedValue(l, true, u, true);
    }

    @Override
    public S gtLte(final T l, final T u) {
        return newWithMutatedValue(l, false, u, true);
    }

    @Override
    public S gteLt(final T l, final T u) {
        return newWithMutatedValue(l, true, u, false);
    }

    private S newWithMutatedValue(
            @Nullable final T l, final boolean includeL,
            @Nullable final T u, final boolean includeU) {
        if (l == null) {
            notNull("u", u);
        } else if (u == null) {
            notNull("l", l);
        }
        if (l instanceof Comparable && u != null && l.getClass().isAssignableFrom(u.getClass())) {
            @SuppressWarnings("unchecked")
            Comparable<T> comparableL = (Comparable<T>) l;
            if (includeL && includeU) {
                isTrueArgument("l must be smaller than or equal to u", comparableL.compareTo(u) <= 0);
            } else {
                isTrueArgument("l must be smaller than u", comparableL.compareTo(u) < 0);
            }
        }
        return newWithMutatedValue(doc -> {
            doc.remove("gte");
            doc.remove("gt");
            doc.remove("lte");
            doc.remove("lt");
            if (l != null) {
                if (includeL) {
                    doc.append("gte", l);
                } else {
                    doc.append("gt", l);
                }
            }
            if (u != null) {
                if (includeU) {
                    doc.append("lte", u);
                } else {
                    doc.append("lt", u);
                }
            }
        });
    }
}
