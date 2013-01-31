/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package com.google.code.morphia.query;

import java.util.List;

/**
 * <p> A nicer interface to the update operations in monogodb. All these operations happen at the server and can cause
 * the server and client version of the Entity to be different </p>
 */
public interface UpdateOperations<T> {
    /**
     * sets the field value
     */
    UpdateOperations<T> set(String fieldExpr, Object value);

    /**
     * removes the field
     */
    UpdateOperations<T> unset(String fieldExpr);

    /**
     * adds the value to an array field
     */
    UpdateOperations<T> add(String fieldExpr, Object value);

    UpdateOperations<T> add(String fieldExpr, Object value, boolean addDups);

    /**
     * adds the values to an array field
     */
    UpdateOperations<T> addAll(String fieldExpr, List<?> values, boolean addDups);

    /**
     * removes the first value from the array
     */
    UpdateOperations<T> removeFirst(String fieldExpr);

    /**
     * removes the last value from the array
     */
    UpdateOperations<T> removeLast(String fieldExpr);

    /**
     * removes the value from the array field
     */
    UpdateOperations<T> removeAll(String fieldExpr, Object value);

    /**
     * removes the values from the array field
     */
    UpdateOperations<T> removeAll(String fieldExpr, List<?> values);

    /**
     * decrements the numeric field by 1
     */
    UpdateOperations<T> dec(String fieldExpr);

    /**
     * increments the numeric field by 1
     */
    UpdateOperations<T> inc(String fieldExpr);

    /**
     * increments the numeric field by value (negatives are allowed)
     */
    UpdateOperations<T> inc(String fieldExpr, Number value);

    /**
     * Turns on validation (for all calls made after); by default validation is on
     */
    UpdateOperations<T> enableValidation();

    /**
     * Turns off validation (for all calls made after)
     */
    UpdateOperations<T> disableValidation();

    /**
     * Enables isolation (so this update happens in one shot, without yielding)
     */
    UpdateOperations<T> isolated();
}
