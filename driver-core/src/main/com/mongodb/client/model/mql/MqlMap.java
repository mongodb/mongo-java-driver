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

package com.mongodb.client.model.mql;

import com.mongodb.annotations.Beta;
import com.mongodb.annotations.Sealed;
import com.mongodb.assertions.Assertions;

import java.util.function.Function;

import static com.mongodb.client.model.mql.MqlValues.of;
import static com.mongodb.client.model.mql.MqlUnchecked.Unchecked.PRESENT;

/**
 * A map {@link MqlValue value} in the context of the MongoDB Query
 * Language (MQL). A map is a finite set of
 * {@link MqlEntry entries} of a certain type.
 * No entry key is repeated. It is a mapping from keys to values.
 *
 * @param <T> the type of the entry values
 * @since 4.9.0
 */
@Sealed
@Beta(Beta.Reason.CLIENT)
public interface MqlMap<T extends MqlValue> extends MqlValue {

    /**
     * Whether {@code this} map has a value (including null) for
     * the provided key.
     *
     * @param key the key.
     * @return the resulting value.
     */
    MqlBoolean has(MqlString key);

    /**
     * Whether {@code this} map has a value (including null) for
     * the provided key.
     *
     * @param key the key.
     * @return the resulting value.
     */
    default MqlBoolean has(final String key) {
        return has(of(key));
    }

    /**
     * The value corresponding to the provided key.
     *
     * <p>Warning: The use of this method is an unchecked assertion that
     * the key is present (which may be confirmed via {@link #has}). See
     * {@link #get(MqlString, MqlValue)} for a typesafe variant.
     *
     * @param key the key.
     * @return the value.
     */
    @MqlUnchecked(PRESENT)
    T get(MqlString key);

    /**
     * The value corresponding to the provided key.
     *
     * <p>Warning: The use of this method is an unchecked assertion that
     * the key is present (which may be confirmed via {@link #has}). See
     * {@link #get(MqlString, MqlValue)} for a typesafe variant.
     *
     * @param key the key.
     * @return the value.
     */
    @MqlUnchecked(PRESENT)
    default T get(final String key) {
        Assertions.notNull("key", key);
        return get(of(key));
    }

    /**
     * The value corresponding to the provided {@code key}, or the
     * {@code other} value if an entry for the key is not
     * {@linkplain #has(MqlString) present}.
     *
     * @param key the key.
     * @param other the other value.
     * @return the resulting value.
     */
    T get(MqlString key, T other);

    /**
     * The value corresponding to the provided {@code key}, or the
     * {@code other} value if an entry for the key is not
     * {@linkplain #has(MqlString) present}.
     *
     * @param key the key.
     * @param other the other value.
     * @return the resulting value.
     */
    default T get(final String key, final T other) {
        Assertions.notNull("key", key);
        Assertions.notNull("other", other);
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
    MqlMap<T> set(MqlString key, T value);

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
    default MqlMap<T> set(final String key, final T value) {
        Assertions.notNull("key", key);
        Assertions.notNull("value", value);
        return set(of(key), value);
    }

    /**
     * Returns a map with the same entries as {@code this} map, but which
     * {@linkplain #has(MqlString) has} no entry with the specified
     * {@code key}.
     *
     * <p>This does not affect the original map.
     *
     * @param key the key.
     * @return the resulting value.
     */
    MqlMap<T> unset(MqlString key);

    /**
     * Returns a map with the same entries as {@code this} map, but which
     * {@linkplain #has(MqlString) has} no entry with the specified
     * {@code key}.
     *
     * <p>This does not affect the original map.
     *
     * @param key the key.
     * @return the resulting value.
     */
    default MqlMap<T> unset(final String key) {
        Assertions.notNull("key", key);
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
    MqlMap<T> merge(MqlMap<? extends T> other);

    /**
     * The {@linkplain MqlEntry entries} of this map as an array.
     * No guarantee is made regarding order.
     *
     * @see MqlArray#asMap
     * @return the resulting value.
     */
    MqlArray<MqlEntry<T>> entrySet();

    /**
     * {@code this} map as a {@linkplain MqlDocument document}.
     *
     * @return the resulting value.
     * @param <R> the resulting type.
     */
    <R extends MqlDocument> R asDocument();

    /**
     * The result of passing {@code this} value to the provided function.
     * Equivalent to {@code f.apply(this)}, and allows lambdas and static,
     * user-defined functions to use the chaining syntax.
     *
     * @see MqlValue#passTo
     * @param f the function to apply.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends MqlValue> R passMapTo(Function<? super MqlMap<T>, ? extends R> f);

    /**
     * The result of applying the provided switch mapping to {@code this} value.
     *
     * @see MqlValue#switchOn
     * @param mapping the switch mapping.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends MqlValue> R switchMapOn(Function<Branches<MqlMap<T>>, ? extends BranchesTerminal<MqlMap<T>, ? extends R>> mapping);
}
