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

import static com.mongodb.assertions.Assertions.notNull;

abstract class RangeConstructibleBsonElement<T, S extends RangeConstructibleBsonElement<T, S>> extends AbstractConstructibleBsonElement<S> {
    RangeConstructibleBsonElement(final String name, final Bson value) {
        super(name, value);
    }

    RangeConstructibleBsonElement(final Bson baseElement, final Bson appendedElementValue) {
        super(baseElement, appendedElementValue);
    }

    public final S score(final SearchScore modifier) {
        return newWithAppendedValue("score", notNull("modifier", modifier));
    }

    final S internalGt(final T l) {
        return newWithMutatedValue(l, false, null, false);
    }

    final S internalLt(final T u) {
        return newWithMutatedValue(null, false, u, false);
    }

    final S internalGte(final T l) {
        return newWithMutatedValue(l, true, null, false);
    }

    final S internalLte(final T u) {
        return newWithMutatedValue(null, false, u, true);
    }

    final S internalGtLt(final T l, final T u) {
        return newWithMutatedValue(l, false, u, false);
    }

    final S internalGteLte(final T l, final T u) {
        return newWithMutatedValue(l, true, u, true);
    }

    final S internalGtLte(final T l, final T u) {
        return newWithMutatedValue(l, false, u, true);
    }

    final S internalGteLt(final T l, final T u) {
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
