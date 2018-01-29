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

/**
 * Apply a function to the input object yielding an appropriate result object. A function may variously provide a mapping between types,
 * object instances or keys and values or any other form of transformation upon the input.
 *
 * @param <T> the type of input objects to the {@code apply} operation
 * @param <R> the type of result objects from the {@code apply} operation. May be the same type as {@code <T>}.
 */
public interface Function<T, R> {

    /**
     * Yield an appropriate result object for the input object.
     *
     * @param t the input object
     * @return the function result
     */
    R apply(T t);
}
