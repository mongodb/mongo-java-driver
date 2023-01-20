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
import static com.mongodb.client.model.expressions.MqlUnchecked.Unchecked.PRESENT;

/**
 * A map {@link Expression value} in the context of the MongoDB Query
 * Language (MQL). An map is a finite, unordered a set of
 * {@link EntryExpression entries} of a certain type, such that no entry key
 * is repeated. It is a mapping from keys to values.
 *
 * @param <T> the type of the elements
 */
public interface MapExpression<T extends Expression> extends Expression {

    /**
     * True if {@code this} map has a value (including null) for
     * the provided key.
     *
     * @param key the key.
     * @return the resulting value.
     */
    BooleanExpression has(StringExpression key);

    /**
     * True if {@code this} map has a value (including null) for
     * the provided key.
     *
     * @param key the key.
     * @return the resulting value.
     */
    default BooleanExpression has(final String key) {
        return has(of(key));
    }

    /**
     * The value corresponding to the provided key.
     *
     * <p>Warning: The use of this method is an unchecked assertion that
     * the key is present (which may be confirmed via {@link #has}). See
     * {@link #get(StringExpression, Expression)} for a typesafe variant.
     *
     * @param key the key.
     * @return the value.
     */
    @MqlUnchecked(PRESENT)
    T get(StringExpression key);

    /**
     * The value corresponding to the provided key.
     *
     * <p>Warning: The use of this method is an unchecked assertion that
     * the key is present (which may be confirmed via {@link #has}). See
     * {@link #get(StringExpression, Expression)} for a typesafe variant.
     *
     * @param key the key.
     * @return the value.
     */
    default T get(final String key) {
        return get(of(key));
    }

    /**
     * The value corresponding to the provided {@code key}, or the
     * {@code other} value if an entry for the key is not present.
     *
     * @param key the key.
     * @param other the other value.
     * @return the resulting value.
     */
    T get(StringExpression key, T other);

    /**
     * The value corresponding to the provided {@code key}, or the
     * {@code other} value if an entry for the key is not present.
     *
     * @param key the key.
     * @param other the other value.
     * @return the resulting value.
     */
    default T get(final String key, final T other) {
        return get(of(key), other);
    }

    /**
     * Returns a map with the same entries as {@code this} map, but with
     * the specified {@code key} set to the specified {@code value}.
     *
     * <p>This does not affect the original map.
     *
     * @param key the key.
     * @param value the value.
     * @return the resulting value.
     */
    MapExpression<T> set(StringExpression key, T value);

    /**
     * Returns a map with the same entries as {@code this} map, but with
     * the specified {@code key} set to the specified {@code value}.
     *
     * <p>This does not affect the original map.
     *
     * @param key the key.
     * @param value the value.
     * @return the resulting value.
     */
    default MapExpression<T> set(final String key, final T value) {
        return set(of(key), value);
    }

    /**
     * Returns a map with the same entries as {@code this} map, but with
     * the specified {@code key} absent.
     *
     * <p>This does not affect the original map.
     *
     * @param key the key.
     * @return the resulting value.
     */
    MapExpression<T> unset(StringExpression key);

    /**
     * Returns a map with the same entries as {@code this} map, but with
     * the specified {@code key} absent.
     *
     * <p>This does not affect the original map.
     *
     * @param key the key.
     * @return the resulting value.
     */
    default MapExpression<T> unset(final String key) {
        return unset(of(key));
    }

    /**
     * Returns a map with the same entries as {@code this} map, but with
     * any keys present in the {@code other} map overwritten with the
     * values of that other map. That is, entries from both this and the
     * other map are merged, with the other map having priority.
     *
     * <p>This does not affect the original map.
     *
     * @param other the other map.
     * @return the resulting value.
     */
    MapExpression<T> merge(MapExpression<? extends T> other);

    /**
     * The {@linkplain EntryExpression entries} of this map as an array.
     *
     * @see ArrayExpression#asMap
     * @return the resulting value.
     */
    ArrayExpression<EntryExpression<T>> entrySet();

    /**
     * {@code this} map as a {@linkplain DocumentExpression document}.
     *
     * @return the resulting value.
     * @param <R> the resulting type.
     */
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
