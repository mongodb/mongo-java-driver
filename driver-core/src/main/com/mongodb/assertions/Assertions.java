/*
 * Copyright 2008-present MongoDB, Inc.
 * Copyright (c) 2008-2014 Atlassian Pty Ltd
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

package com.mongodb.assertions;

import com.mongodb.internal.async.SingleResultCallback;

import java.util.Collection;

/**
 * <p>Design by contract assertions.</p> <p>This class is not part of the public API and may be removed or changed at any time.</p>
 */
public final class Assertions {
    /**
     * Throw IllegalArgumentException if the value is null.
     *
     * @param name  the parameter name
     * @param value the value that should not be null
     * @param <T>   the value type
     * @return the value
     * @throws java.lang.IllegalArgumentException if value is null
     */
    public static <T> T notNull(final String name, final T value) {
        if (value == null) {
            throw new IllegalArgumentException(name + " can not be null");
        }
        return value;
    }

    /**
     * Throw IllegalArgumentException if the value is null.
     *
     * @param name  the parameter name
     * @param value the value that should not be null
     * @param callback  the callback that also is passed the exception if the value is null
     * @param <T>   the value type
     * @return the value
     * @throws java.lang.IllegalArgumentException if value is null
     */
    public static <T> T notNull(final String name, final T value, final SingleResultCallback<?> callback) {
        if (value == null) {
            IllegalArgumentException exception = new IllegalArgumentException(name + " can not be null");
            callback.onResult(null, exception);
            throw exception;
        }
        return value;
    }

    /**
     * Throw IllegalStateException if the condition if false.
     *
     * @param name      the name of the state that is being checked
     * @param condition the condition about the parameter to check
     * @throws java.lang.IllegalStateException if the condition is false
     */
    public static void isTrue(final String name, final boolean condition) {
        if (!condition) {
            throw new IllegalStateException("state should be: " + name);
        }
    }

    /**
     * Throw IllegalStateException if the condition if false.
     *
     * @param name      the name of the state that is being checked
     * @param condition the condition about the parameter to check
     * @param callback  the callback that also is passed the exception if the condition is not true
     * @throws java.lang.IllegalStateException if the condition is false
     */
    public static void isTrue(final String name, final boolean condition, final SingleResultCallback<?> callback) {
        if (!condition) {
            IllegalStateException exception = new IllegalStateException("state should be: " + name);
            callback.onResult(null, exception);
            throw exception;
        }
    }

    /**
     * Throw IllegalArgumentException if the condition if false.
     *
     * @param name      the name of the state that is being checked
     * @param condition the condition about the parameter to check
     * @throws java.lang.IllegalArgumentException if the condition is false
     */
    public static void isTrueArgument(final String name, final boolean condition) {
        if (!condition) {
            throw new IllegalArgumentException("state should be: " + name);
        }
    }

    /**
     * Throw IllegalArgumentException if the collection contains a null value.
     *
     * @param name       the name of the collection
     * @param collection the collection
     * @throws java.lang.IllegalArgumentException if the collection contains a null value
     */
    public static void doesNotContainNull(final String name, final Collection<?> collection) {
        // Use a loop instead of the contains method, as some implementations of that method will throw an exception if passed null as a
        // parameter (in particular, lists returned by List.of methods)
        for (Object o : collection) {
            if (o == null) {
                throw new IllegalArgumentException(name + " can not contain a null value");
            }
        }
    }

    private Assertions() {
    }
}
