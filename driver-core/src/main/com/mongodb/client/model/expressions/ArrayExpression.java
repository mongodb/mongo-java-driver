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

import java.util.function.BinaryOperator;
import java.util.function.Function;

import static com.mongodb.client.model.expressions.Expressions.of;

/**
 * Expresses an array value. An array value is a finite, ordered collection of
 * elements of a certain type.
 *
 * @param <T> the type of the elements in the array
 */
public interface ArrayExpression<T extends Expression> extends Expression {

    /**
     * Returns an array consisting of those elements in this array that match
     * the given predicate condition. Evaluates each expression in this array
     * according to the cond function. If cond evaluates to logical true, then
     * the element is preserved; if cond evaluates to logical false, the element
     * is omitted.
     *
     * @param cond the function to apply to each element
     * @return the new array
     */
    ArrayExpression<T> filter(Function<? super T, ? extends BooleanExpression> cond);

    /**
     * Returns an array consisting of the results of applying the given function
     * to the elements of this array.
     *
     * @param in the function to apply to each element
     * @return the new array
     * @param <R> the type contained in the resulting array
     */
    <R extends Expression> ArrayExpression<R> map(Function<? super T, ? extends R> in);

    /**
     * Performs a reduction on the elements of this array, using the provided
     * identity value and an associative reducing function, and returns
     * the reduced value. The initial value must be the identity value for the
     * reducing function.
     *
     * @param initialValue the identity for the reducing function
     * @param in the associative reducing function
     * @return the reduced value
     */
    T reduce(T initialValue, BinaryOperator<T> in);

    IntegerExpression size();

    T arrayElemAt(IntegerExpression i);

    default T arrayElemAt(final int i) {
        return this.arrayElemAt(of(i));
    }

    T first();

    T last();

    BooleanExpression contains(T contains);

    ArrayExpression<T> concatArrays(ArrayExpression<T> array);

    ArrayExpression<T> slice(IntegerExpression start, IntegerExpression length);

    default ArrayExpression<T> slice(final int start, final int length) {
        return this.slice(of(start), of(length));
    }

    ArrayExpression<T> setUnion(ArrayExpression<T> set);

    ArrayExpression<T> distinct();
}
