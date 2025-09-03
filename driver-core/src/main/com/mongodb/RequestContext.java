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

package com.mongodb;

import com.mongodb.lang.Nullable;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * The request context, useful for implementing distributed tracing.
 *
 * @see MongoClientSettings#getContextProvider()
 * @since 4.4
 */
public interface RequestContext {
    /**
     * Resolve a value given a key that exists within the {@link RequestContext}, or throw
     * a {@link NoSuchElementException} if the key is not present.
     *
     * @param key a lookup key to resolve the value within the context
     * @param <T> an unchecked casted generic for fluent typing convenience
     * @return the value resolved for this key (throws if key not found)
     * @throws NoSuchElementException when the given key is not present
     * @see #getOrDefault(Object, Object)
     * @see #getOrEmpty(Object)
     * @see #hasKey(Object)
     */
    <T> T get(Object key);

    /**
     * Resolve a value given a type key within the {@link RequestContext}.
     *
     * @param key a type key to resolve the value within the context
     * @param <T> an unchecked casted generic for fluent typing convenience
     * @return the value resolved for this type key (throws if key not found)
     * @throws NoSuchElementException when the given type key is not present
     * @see #getOrDefault(Object, Object)
     * @see #getOrEmpty(Object)
     */
    default <T> T get(Class<T> key) {
        T v = get((Object) key);
        if (key.isInstance(v)) {
            return v;
        }
        throw new NoSuchElementException("Context does not contain a value of type " + key
                .getName());
    }

    /**
     * Resolve a value given a key within the {@link RequestContext}. If unresolved return the
     * passed default value.
     *
     * @param <T> an unchecked casted generic for fluent typing convenience
     * @param key          a lookup key to resolve the value within the context
     * @param defaultValue a fallback value if key doesn't resolve
     * @return the value resolved for this key, or the given default if not present
     */
    @Nullable
    default <T> T getOrDefault(Object key, @Nullable T defaultValue) {
        if (!hasKey(key)) {
            return defaultValue;
        }
        return get(key);
    }

    /**
     * Resolve a value given a key within the {@link RequestContext}.
     *
     * @param <T> an unchecked casted generic for fluent typing convenience
     * @param key a lookup key to resolve the value within the context
     * @return an {@link Optional} of the value for that key.
     */
    default <T> Optional<T> getOrEmpty(Object key) {
        if (hasKey(key)) {
            return Optional.of(get(key));
        }
        return Optional.empty();
    }

    /**
     * Return true if a particular key resolves to a value within the {@link RequestContext}.
     *
     * @param key a lookup key to test for
     * @return true if this context contains the given key
     */
    boolean hasKey(Object key);

    /**
     * Return true if the {@link RequestContext} is empty.
     *
     * @return true if the {@link RequestContext} is empty.
     */
    boolean isEmpty();

    /**
     * Modifies this instance with the given key and value. If that key existed in the current {@link RequestContext}, its associated
     * value is replaced.
     *
     * @param key   the key to add/update
     * @param value the value to associate to the key
     * @throws NullPointerException if either the key or value are null
     */
    void put(Object key, Object value);

    /**
     * Modifies this instance with the given key and value <strong>only if the value is not {@literal null}</strong>. If that key existed
     * in the current Context, its associated value is replaced in the resulting {@link RequestContext}.
     *
     * @param key         the key to add/update
     * @param valueOrNull the value to associate to the key, null to ignore the operation
     * @throws NullPointerException if the key is null
     */
    default void putNonNull(Object key, @Nullable Object valueOrNull) {
        if (valueOrNull != null) {
            put(key, valueOrNull);
        }
    }

    /**
     * Delete the given key and its associated value from the RequestContext.
     *
     * @param key the key to remove.
     */
    void delete(Object key);

    /**
     * Return the size of this {@link RequestContext}, the number of key/value pairs stored inside it.
     *
     * @return the size of the {@link RequestContext}
     */
    int size();

    /**
     * Stream key/value pairs from this {@link RequestContext}
     *
     * <p>
     * It is not specified whether modification of a {@code Map.Entry} instance in the {@code Stream} results in a modification of the
     * state of the {@code RequestContext}, or whether the {@code Map.Entry} instances are modifiable. That is considered an
     * implementation detail, so users of this method should not rely on the behavior one way or the other unless the implementing class
     * has documented it.
     * </p>
     *
     * @return a {@link Stream} of key/value pairs held by this context
     */
    Stream<Map.Entry<Object, Object>> stream();
}
