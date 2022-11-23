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

public interface MapExpression<T extends Expression> extends Expression {

    T get(StringExpression key);

    default T get(final String key) {
        return get(of(key));
    }

    T get(StringExpression key, T orElse);

    default T get(final String key, final T orElse) {
        return get(of(key), orElse);
    }

    MapExpression<T> set(StringExpression key, T value);

    default MapExpression<T> set(final String key, final T value) {
        return set(of(key), value);
    }

    MapExpression<T> unset(StringExpression key);

    default MapExpression<T> unset(final String key) {
        return unset(of(key));
    }

    MapExpression<T> mergee(MapExpression<T> map);

    ArrayExpression<EntryExpression<T>> entrySet();
}
