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
 * An array {@link Expression value} in the context of the MongoDB Query
 * Language (MQL). An array is a finite, ordered collection of elements of a
 * certain type. It is also known as a finite mathematical sequence.
 *
 * @param <T> the type of the elements
 */
public interface ArrayExpression<T extends Expression> extends Expression {

    /**
     * An array consisting of only those elements in {@code this} array that
     * match the provided predicate.
     *
     * @param predicate the predicate to apply to each element to determine if
     *                  it should be included.
     * @return the resulting array.
     */
    ArrayExpression<T> filter(Function<? super T, ? extends BooleanExpression> predicate);

    /**
     * An array consisting of the results of applying the provided function to
     * the elements of {@code this} array.
     *
     * @param in the function to apply to each element.
     * @return the resulting array.
     * @param <R> the type of the elements of the resulting array.
     */
    <R extends Expression> ArrayExpression<R> map(Function<? super T, ? extends R> in);

    /**
     * The size of {@code this} array.
     *
     * @return the size.
     */
    IntegerExpression size();

    /**
     * True if any value in {@code this} array satisfies the predicate.
     *
     * @param predicate the predicate.
     * @return the resulting value.
     */
    BooleanExpression any(Function<? super T, BooleanExpression> predicate);

    /**
     * True if all values in {@code this} array satisfy the predicate.
     *
     * @param predicate the predicate.
     * @return the resulting value.
     */
    BooleanExpression all(Function<? super T, BooleanExpression> predicate);

    /**
     * The sum of adding together all the values of {@code this} array,
     * via the provided {@code mapper}. Returns 0 if the array is empty.
     *
     * <p>The mapper may be used to transform the values of {@code this} array
     * into {@linkplain NumberExpression numbers}. If no transformation is
     * necessary, then the identify function {@code array.sum(v -> v)} should
     * be used.
     *
     * @param mapper the mapper function.
     * @return the resulting value.
     */
    NumberExpression sum(Function<? super T, ? extends NumberExpression> mapper);

    /**
     * The product of multiplying together all the values of {@code this} array,
     * via the provided {@code mapper}. Returns 1 if the array is empty.
     *
     * <p>The mapper may be used to transform the values of {@code this} array
     * into {@linkplain NumberExpression numbers}. If no transformation is
     * necessary, then the identify function {@code array.multiply(v -> v)}
     * should be used.
     *
     * @param mapper the mapper function.
     * @return the resulting value.
     */
    NumberExpression multiply(Function<? super T, ? extends NumberExpression> mapper);

    /**
     * The {@linkplain #gt(Expression) largest} value all the values of
     * {@code this} array, or the {@code other} value if this array is empty.
     *
     * @mongodb.server.release 5.2
     * @param other the other value.
     * @return the resulting value.
     */
    T max(T other);

    /**
     * The {@linkplain #lt(Expression) smallest} value all the values of
     * {@code this} array, or the {@code other} value if this array is empty.
     *
     * @mongodb.server.release 5.2
     * @param other the other value.
     * @return the resulting value.
     */
    T min(T other);

    /**
     * The {@linkplain #gt(Expression) largest} {@code n} elements of
     * {@code this} array, or all elements if the array contains fewer than
     * {@code n} elements.
     *
     * @mongodb.server.release 5.2
     * @param n the number of elements.
     * @return the resulting value.
     */
    ArrayExpression<T> maxN(IntegerExpression n);

    /**
     * The {@linkplain #lt(Expression) smallest} {@code n} elements of
     * {@code this} array, or all elements if the array contains fewer than
     * {@code n} elements.
     *
     * @mongodb.server.release 5.2
     * @param n the number of elements.
     * @return the resulting value.
     */
    ArrayExpression<T> minN(IntegerExpression n);

    /**
     * The string-concatenation of all the values of {@code this} array,
     * via the provided {@code mapper}. Returns the empty string if the array
     * is empty.
     *
     * <p>The mapper may be used to transform the values of {@code this} array
     * into {@linkplain StringExpression strings}. If no transformation is
     * necessary, then the identify function {@code array.join(v -> v)} should
     * be used.
     *
     * @param mapper the mapper function.
     * @return the resulting value.
     */
    StringExpression join(Function<? super T, StringExpression> mapper);

    /**
     * The array-concatenation of all the array values of {@code this} array,
     * via the provided {@code mapper}. Returns the empty array if the array
     * is empty.
     *
     * <p>The mapper may be used to transform the values of {@code this} array
     * into {@linkplain ArrayExpression arrays}. If no transformation is
     * necessary, then the identify function {@code array.concat(v -> v)} should
     * be used.
     *
     * @param mapper the mapper function.
     * @return the resulting value.
     * @param <R> the type of the elements of the array.
     */
    <R extends Expression> ArrayExpression<R> concat(Function<? super T, ? extends ArrayExpression<? extends R>> mapper);

    /**
     * The set union of all the array values of {@code this} array,
     * via the provided {@code mapper}. Returns the empty array if the array
     * is empty.
     *
     * <p>The mapper may be used to transform the values of {@code this} array
     * into {@linkplain ArrayExpression arrays}. If no transformation is
     * necessary, then the identify function {@code array.union(v -> v)} should
     * be used.
     *
     * @param mapper the mapper function.
     * @return the resulting value.
     * @param <R> the type of the elements of the array.
     */
    <R extends Expression> ArrayExpression<R> union(Function<? super T, ? extends ArrayExpression<? extends R>> mapper);

    /**
     * The {@linkplain MapExpression map} value corresponding to the
     * {@linkplain EntryExpression entry} values of {@code this} array,
     * via the provided {@code mapper}. Returns the empty array if the array
     * is empty.
     *
     * <p>The mapper may be used to transform the values of {@code this} array
     * into {@linkplain EntryExpression entries}. If no transformation is
     * necessary, then the identify function {@code array.union(v -> v)} should
     * be used.
     *
     * @see MapExpression#entrySet()
     * @param mapper the mapper function.
     * @return the resulting value.
     * @param <R> the type of the resulting map's values.
     */
    <R extends Expression> MapExpression<R> asMap(Function<? super T, ? extends EntryExpression<? extends R>> mapper);

    /**
     * Returns the element at the provided index {@code i} for
     * {@code this} array.
     *
     * <p>Warning: The use of this method is an assertion that
     * the index {@code i} is in bounds for the array.
     * If the index is out of bounds for this array, then
     * the behaviour of the API is not defined.
     *
     * @param i the index.
     * @return the resulting value.
     */
    @MqlUnchecked(PRESENT)
    T elementAt(IntegerExpression i);

    /**
     * Returns the element at the provided index {@code i} for
     * {@code this} array.
     *
     * <p>Warning: The use of this method is an assertion that
     * the index {@code i} is in bounds for the array.
     * If the index is out of bounds for this array, then
     * the behaviour of the API is not defined.
     *
     * @param i the index.
     * @return the resulting value.
     */
    @MqlUnchecked(PRESENT)
    default T elementAt(final int i) {
        return this.elementAt(of(i));
    }

    /**
     * Returns the first element of {@code this} array.
     *
     * <p>Warning: The use of this method is an assertion that
     * the array is not empty.
     * If the array is empty then the behaviour of the API is not defined.
     *
     * @mongodb.server.release 4.4
     * @return the resulting value.
     */
    @MqlUnchecked(PRESENT)
    T first();

    /**
     * Returns the last element of {@code this} array.
     *
     * <p>Warning: The use of this method is an assertion that
     * the array is not empty.
     * If the array is empty then the behaviour of the API is not defined.
     *
     * @mongodb.server.release 4.4
     * @return the resulting value.
     */
    T last();

    /**
     * True if {@code this} array contains a value that is
     * {@linkplain #eq equal} to the provided {@code value}.
     *
     * @param value the value.
     * @return the resulting value.
     */
    BooleanExpression contains(T value);

    /**
     * The result of concatenating {@code this} array first with
     * the {@code other} array ensuing.
     *
     * @param other the other array.
     * @return the resulting array.
     */
    ArrayExpression<T> concat(ArrayExpression<? extends T> other);

    /**
     * The subarray of {@code this} array, from the {@code start} index
     * inclusive, and continuing for the specified {@code length}, up to
     * the end of the array.
     *
     * @param start start index
     * @param length length
     * @return the resulting value
     */
    ArrayExpression<T> slice(IntegerExpression start, IntegerExpression length);

    /**
     * The subarray of {@code this} array, from the {@code start} index
     * inclusive, and continuing for the specified {@code length}, or
     * to the end of the array.
     *
     * @param start start index
     * @param length length
     * @return the resulting value
     */
    default ArrayExpression<T> slice(final int start, final int length) {
        return this.slice(of(start), of(length));
    }

    /**
     * The set-union of {@code this} array and the {@code other} array ensuing,
     * containing only the distinct values of both.
     * No guarantee is made regarding order.
     *
     * @param other the other array.
     * @return the resulting array.
     */
    ArrayExpression<T> union(ArrayExpression<? extends T> other);


    /**
     * An array containing only the distinct values of {@code this} array.
     * No guarantee is made regarding order.
     *
     * @return the resulting value
     */
    ArrayExpression<T> distinct();

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
    <R extends Expression> R passArrayTo(Function<? super ArrayExpression<T>, ? extends R> f);

    /**
     * The result of applying the provided switch mapping to {@code this} value.
     *
     * @see Expression#switchOn
     * @param mapping the switch mapping.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends Expression> R switchArrayOn(Function<Branches<ArrayExpression<T>>, ? extends BranchesTerminal<ArrayExpression<T>, ? extends R>> mapping);
}
