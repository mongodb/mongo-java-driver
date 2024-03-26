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
import com.mongodb.lang.Nullable;

import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * <p>Design by contract assertions.</p> <p>This class is not part of the public API and may be removed or changed at any time.</p>
 * All {@code assert...} methods throw {@link AssertionError} and should be used to check conditions which may be violated if and only if
 * the driver code is incorrect. The intended usage of this methods is the same as of the
 * <a href="https://docs.oracle.com/javase/8/docs/technotes/guides/language/assert.html">Java {@code assert} statement</a>. The reason
 * for not using the {@code assert} statements is that they are not always enabled. We prefer having internal checks always done at the
 * cost of our code doing a relatively small amount of additional work in production.
 * The {@code assert...} methods return values to open possibilities of being used fluently.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
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
     * Throw IllegalArgumentException if the values is null or contains null.
     *
     * <p><b>Note:</b> If performance is a concern, consider deferring the integrity validation
     * to the point of actual data iteration to avoid incurring additional reference chasing for collections of complex objects.
     * However, if performance considerations are low and it is acceptable to iterate over the data twice,
     * this method can still be used for validation purposes.
     *
     * @param name   the parameter name.
     * @param values the values that should not contain null elements.
     * @param <T>    the type of elements in the collection.
     * @return the input collection if it passes the null element validation.
     * @throws java.lang.IllegalArgumentException if the input collection is null or contains null elements.
     */
    public static <T> Iterable<T> notNullElements(final String name, final Iterable<T> values) {
        if (values == null) {
            throw new IllegalArgumentException(name + " can not be null");
        }

        for (T value : values) {
            if (value == null){
                throw new IllegalArgumentException(name + " can not contain null");
            }
        }

        return values;
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
            callback.completeExceptionally(exception);
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
            callback.completeExceptionally(exception);
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
     * Throw IllegalArgumentException if the condition returns false.
     *
     * @param msg the error message if the condition returns false
     * @param supplier the supplier of the value
     * @param condition the condition function
     * @return the supplied value if it meets the condition
     * @param <T> the type of the supplied value
     */
    public static <T> T isTrueArgument(final String msg, final Supplier<T> supplier, final Function<T, Boolean> condition) {
        T value = doesNotThrow(supplier);
        if (!condition.apply(value)) {
            throw new IllegalArgumentException(msg);
        }

        return value;
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

    /**
     * @param value A value to check.
     * @param <T> The type of {@code value}.
     * @return {@code null}.
     * @throws AssertionError If {@code value} is not {@code null}.
     */
    @Nullable
    public static <T> T assertNull(@Nullable final T value) throws AssertionError {
        if (value != null) {
            throw new AssertionError(value.toString());
        }
        return null;
    }

    /**
     * @param value A value to check.
     * @param <T> The type of {@code value}.
     * @return {@code value}
     * @throws AssertionError If {@code value} is {@code null}.
     */
    public static <T> T assertNotNull(@Nullable final T value) throws AssertionError {
        if (value == null) {
            throw new AssertionError();
        }
        return value;
    }

    /**
     * @param value A value to check.
     * @return {@code true}.
     * @throws AssertionError If {@code value} is {@code false}.
     */
    public static boolean assertTrue(final boolean value) throws AssertionError {
        if (!value) {
            throw new AssertionError();
        }
        return true;
    }

    /**
     * @param value A value to check.
     * @return {@code false}.
     * @throws AssertionError If {@code value} is {@code true}.
     */
    public static boolean assertFalse(final boolean value) throws AssertionError {
        if (value) {
            throw new AssertionError();
        }
        return false;
    }

    /**
     * @throws AssertionError Always
     * @return Never completes normally. The return type is {@link AssertionError} to allow writing {@code throw fail()}.
     * This may be helpful in non-{@code void} methods.
     */
    public static AssertionError fail() throws AssertionError {
        throw new AssertionError();
    }

    /**
     * @param msg The failure message.
     * @throws AssertionError Always
     * @return Never completes normally. The return type is {@link AssertionError} to allow writing {@code throw fail("failure message")}.
     * This may be helpful in non-{@code void} methods.
     */
    public static AssertionError fail(final String msg) throws AssertionError {
        throw new AssertionError(assertNotNull(msg));
    }

    /**
     * @param supplier the supplier to check
     * @return {@code supplier.get()}
     * @throws AssertionError If {@code supplier.get()} throws an exception
     */
    public static <T> T doesNotThrow(final Supplier<T> supplier) throws AssertionError {
        try {
            return supplier.get();
        } catch (Exception e) {
            throw new AssertionError(e.getMessage(), e);
        }
    }

    private Assertions() {
    }
}
