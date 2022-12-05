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

package com.mongodb.client.model.expressions;

import static com.mongodb.client.model.expressions.Expressions.of;

/**
 * A map entry {@linkplain Expression value} in the context
 * of the MongoDB Query Language (MQL). An entry has a
 * {@linkplain StringExpression string} key and some
 * {@linkplain Expression value}. Entries are used with
 * {@linkplain MapExpression maps}.
 *
 * Entries are {@linkplain Expression#isDocumentOr document-like} and
 * {@linkplain Expression#isMapOr map-like}, unless the method returning the
 * entry specifies otherwise.
 *
 * @param <T> The type of the value
 */
public interface EntryExpression<T extends Expression> extends Expression {
    StringExpression getKey();

    T getValue();

    EntryExpression<T> setValue(T val);

    EntryExpression<T> setKey(StringExpression key);
    default EntryExpression<T> setKey(final String key) {
        return setKey(of(key));
    }
}
