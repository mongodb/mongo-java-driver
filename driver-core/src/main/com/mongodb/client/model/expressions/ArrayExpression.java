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

    BooleanExpression any(Function<? super T, BooleanExpression> predicate);

    BooleanExpression all(Function<? super T, BooleanExpression> predicate);

    NumberExpression sum(Function<? super T, ? extends NumberExpression> mapper);

    NumberExpression multiply(Function<? super T, ? extends NumberExpression> mapper);

    T max(T other);

    T min(T other);

    ArrayExpression<T> maxN(IntegerExpression n);
    ArrayExpression<T> minN(IntegerExpression n);

    StringExpression join(Function<? super T, StringExpression> mapper);

    <R extends Expression> ArrayExpression<R> concat(Function<? super T, ? extends ArrayExpression<? extends R>> mapper);

    <R extends Expression> ArrayExpression<R> union(Function<? super T, ? extends ArrayExpression<? extends R>> mapper);

    <R extends Expression> MapExpression<R> asMap(Function<? super T, ? extends EntryExpression<? extends R>> mapper);

    /**
     * user asserts that i is in bounds for the array
     *
     * @param i
     * @return
     */
    T elementAt(IntegerExpression i);

    default T elementAt(final int i) {
        return this.elementAt(of(i));
    }

    /**
     * user asserts that array is not empty
     * @return
     */
    T first();

    /**
     * user asserts that array is not empty
     * @return
     */
    T last();

    BooleanExpression contains(T contains);

    ArrayExpression<T> concat(ArrayExpression<? extends T> array);

    ArrayExpression<T> slice(IntegerExpression start, IntegerExpression length);

    default ArrayExpression<T> slice(final int start, final int length) {
        return this.slice(of(start), of(length));
    }

    ArrayExpression<T> union(ArrayExpression<? extends T> set);

    ArrayExpression<T> distinct();

    <R extends Expression> R passArrayTo(Function<? super ArrayExpression<T>, ? extends R> f);

    <R extends Expression> R switchArrayOn(Function<Branches<ArrayExpression<T>>, ? extends BranchesTerminal<ArrayExpression<T>, ? extends R>> on);
}
