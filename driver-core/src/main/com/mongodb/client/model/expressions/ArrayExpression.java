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

import java.util.function.BiFunction;
import java.util.function.Function;

public interface ArrayExpression<T extends Expression> extends Expression {

    /**
     * Returns an array consisting of those elements in this array that match
     * the given predicate condition. Evaluates each expression in this array
     * using the cond function, and keeps only those evaluating to logical true.
     *
     * If cond evaluates to a value that is not logical true or false, then it
     * is undefined whether that particular element will be preserved.
     *
     * @param cond the function to apply to each element
     * @return the new array
     */
    ArrayExpression<T> filter(Function<T, BooleanExpression> cond);

    /**
     * Returns an array consisting of the results of applying the given function
     * to the elements of this array.
     *
     * @param in the function to apply to each element
     * @return the new array
     * @param <R>
     */
    <R extends Expression> ArrayExpression<R> map(Function<T, ? extends R> in);

    /**
     * Performs a reduction on the elements of this array, using the provided
     * identity value and an associative accumulation function, and returns
     * the reduced value. The initial value must be the identity value for the
     * reducing function.
     *
     * @param initialValue the identity for the reducing function
     * @param in the associative accumulation function
     * @return the reduced value
     */
    T reduce(T initialValue, BiFunction<T,  T,  T> in);

}
