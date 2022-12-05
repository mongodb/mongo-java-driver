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

import java.util.function.Function;

import static com.mongodb.client.model.expressions.Expressions.of;

public interface MapExpression<T extends Expression> extends Expression {

    BooleanExpression has(StringExpression key);

    default BooleanExpression has(String key) {
        return has(of(key));
    }

    // TODO-END doc "user asserts"
    T get(StringExpression key);

    // TODO-END doc "user asserts"
    default T get(final String key) {
        return get(of(key));
    }

    T get(StringExpression key, T other);

    default T get(final String key, final T other) {
        return get(of(key), other);
    }

    MapExpression<T> set(StringExpression key, T value);

    default MapExpression<T> set(final String key, final T value) {
        return set(of(key), value);
    }

    MapExpression<T> unset(StringExpression key);

    default MapExpression<T> unset(final String key) {
        return unset(of(key));
    }

    MapExpression<T> merge(MapExpression<? extends T> map);

    ArrayExpression<EntryExpression<T>> entrySet();

    <R extends DocumentExpression> R asDocument();

    /**
     * The result of passing {@code this} value to the provided function.
     * Equivalent to {@code f.apply(this)}, and allows lambdas and static,
     * user-defined functions to use the chaining syntax.
     *
     * @see Expression#passTo
     * @param f the function to apply.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends Expression> R passMapTo(Function<? super MapExpression<T>, ? extends R> f);

    /**
     * The result of applying the provided switch mapping to {@code this} value.
     *
     * @see Expression#switchOn
     * @param mapping the switch mapping.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends Expression> R switchMapOn(Function<Branches<MapExpression<T>>, ? extends BranchesTerminal<MapExpression<T>, ? extends R>> mapping);
}
