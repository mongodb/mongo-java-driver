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

import com.mongodb.annotations.Beta;
import com.mongodb.assertions.Assertions;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.mongodb.client.model.expressions.MqlUnchecked.Unchecked.TYPE_ARGUMENT;

/**
 * See {@link Branches}.
 *
 * @param <T> the type of the values that may be checked.
 * @param <R> the type of the value produced.
 * @since 4.9.0
 */
@Beta(Beta.Reason.CLIENT)
public final class BranchesIntermediary<T extends Expression, R extends Expression> extends BranchesTerminal<T, R> {
    BranchesIntermediary(final List<Function<T, SwitchCase<R>>> branches) {
        super(branches, null);
    }

    private BranchesIntermediary<T, R> with(final Function<T, SwitchCase<R>> switchCase) {
        List<Function<T, SwitchCase<R>>> v = new ArrayList<>(this.getBranches());
        v.add(switchCase);
        return new BranchesIntermediary<>(v);
    }

    private static <T extends Expression> MqlExpression<?> mqlEx(final T value) {
        return (MqlExpression<?>) value;
    }

    // is fn

    /**
     * A successful check for the specified {@code predicate}
     * produces a value specified by the {@code mapping}.
     *
     * @param predicate the predicate.
     * @param mapping the mapping.
     * @return the appended sequence of checks.
     */
    public BranchesIntermediary<T, R> is(final Function<? super T, BooleanExpression> predicate, final Function<? super T, ? extends R> mapping) {
        Assertions.notNull("predicate", predicate);
        Assertions.notNull("mapping", mapping);
        return this.with(value -> new SwitchCase<>(predicate.apply(value), mapping.apply(value)));
    }

    // eq lt lte

    /**
     * A successful check for {@linkplain Expression#eq equality}
     * produces a value specified by the {@code mapping}.
     *
     * @param v the value to check against.
     * @param mapping the mapping.
     * @return the appended sequence of checks.
     */
    public BranchesIntermediary<T, R> eq(final T v, final Function<? super T, ? extends R> mapping) {
        Assertions.notNull("v", v);
        Assertions.notNull("mapping", mapping);
        return is(value -> value.eq(v), mapping);
    }

    /**
     * A successful check for being
     * {@linkplain Expression#lt less than}
     * the provided value {@code v}
     * produces a value specified by the {@code mapping}.
     *
     * @param v the value to check against.
     * @param mapping the mapping.
     * @return the appended sequence of checks.
     */
    public BranchesIntermediary<T, R> lt(final T v, final Function<? super T, ? extends R> mapping) {
        Assertions.notNull("v", v);
        Assertions.notNull("mapping", mapping);
        return is(value -> value.lt(v), mapping);
    }

    /**
     * A successful check for being
     * {@linkplain Expression#lte less than or equal to}
     * the provided value {@code v}
     * produces a value specified by the {@code mapping}.
     *
     * @param v the value to check against.
     * @param mapping the mapping.
     * @return the appended sequence of checks.
     */
    public BranchesIntermediary<T, R> lte(final T v, final Function<? super T, ? extends R> mapping) {
        Assertions.notNull("v", v);
        Assertions.notNull("mapping", mapping);
        return is(value -> value.lte(v), mapping);
    }

    // is type

    /**
     * A successful check for
     * {@linkplain Expression#isBooleanOr(BooleanExpression) being a boolean}
     * produces a value specified by the {@code mapping}.
     *
     * @param mapping the mapping.
     * @return the appended sequence of checks.
     */
    public BranchesIntermediary<T, R> isBoolean(final Function<? super BooleanExpression, ? extends R> mapping) {
        Assertions.notNull("mapping", mapping);
        return is(v -> mqlEx(v).isBoolean(), v -> mapping.apply((BooleanExpression) v));
    }

    /**
     * A successful check for
     * {@linkplain Expression#isNumberOr(NumberExpression) being a number}
     * produces a value specified by the {@code mapping}.
     *
     * @mongodb.server.release 4.4
     * @param mapping the mapping.
     * @return the appended sequence of checks.
     */
    public BranchesIntermediary<T, R> isNumber(final Function<? super NumberExpression, ? extends R> mapping) {
        Assertions.notNull("mapping", mapping);
        return is(v -> mqlEx(v).isNumber(), v -> mapping.apply((NumberExpression) v));
    }

    /**
     * A successful check for
     * {@linkplain Expression#isIntegerOr(IntegerExpression) being an integer}
     * produces a value specified by the {@code mapping}.
     *
     * @mongodb.server.release 4.4
     * @param mapping the mapping.
     * @return the appended sequence of checks.
     */
    public BranchesIntermediary<T, R> isInteger(final Function<? super IntegerExpression, ? extends R> mapping) {
        Assertions.notNull("mapping", mapping);
        return is(v -> mqlEx(v).isInteger(), v -> mapping.apply((IntegerExpression) v));
    }

    /**
     * A successful check for
     * {@linkplain Expression#isStringOr(StringExpression) being a string}
     * produces a value specified by the {@code mapping}.
     *
     * @param mapping the mapping.
     * @return the appended sequence of checks.
     */
    public BranchesIntermediary<T, R> isString(final Function<? super StringExpression, ? extends R> mapping) {
        Assertions.notNull("mapping", mapping);
        return is(v -> mqlEx(v).isString(), v -> mapping.apply((StringExpression) v));
    }

    /**
     * A successful check for
     * {@linkplain Expression#isDateOr(DateExpression) being a date}
     * produces a value specified by the {@code mapping}.
     *
     * @param mapping the mapping.
     * @return the appended sequence of checks.
     */
    public BranchesIntermediary<T, R> isDate(final Function<? super DateExpression, ? extends R> mapping) {
        Assertions.notNull("mapping", mapping);
        return is(v -> mqlEx(v).isDate(), v -> mapping.apply((DateExpression) v));
    }

    /**
     * A successful check for
     * {@linkplain Expression#isArrayOr(ArrayExpression) being an array}
     * produces a value specified by the {@code mapping}.
     *
     * <p>Warning: The type argument of the array is not
     * enforced by the API. The use of this method is an
     * unchecked assertion that the type argument is correct.
     *
     * @param mapping the mapping.
     * @return the appended sequence of checks.
     * @param <Q> the type of the elements of the resulting array.
     */
    @SuppressWarnings("unchecked")
    public <Q extends Expression> BranchesIntermediary<T, R> isArray(final Function<? super ArrayExpression<@MqlUnchecked(TYPE_ARGUMENT) Q>, ? extends R> mapping) {
        Assertions.notNull("mapping", mapping);
        return is(v -> mqlEx(v).isArray(), v -> mapping.apply((ArrayExpression<Q>) v));
    }

    /**
     * A successful check for
     * {@linkplain Expression#isDocumentOr(DocumentExpression) being a document}
     * (or document-like value, see
     * {@link MapExpression} and {@link EntryExpression})
     * produces a value specified by the {@code mapping}.
     *
     * @param mapping the mapping.
     * @return the appended sequence of checks.
     */
    public BranchesIntermediary<T, R> isDocument(final Function<? super DocumentExpression, ? extends R> mapping) {
        Assertions.notNull("mapping", mapping);
        return is(v -> mqlEx(v).isDocumentOrMap(), v -> mapping.apply((DocumentExpression) v));
    }

    /**
     * A successful check for
     * {@linkplain Expression#isMapOr(MapExpression) being a map}
     * (or map-like value, see
     * {@link DocumentExpression} and {@link EntryExpression})
     * produces a value specified by the {@code mapping}.
     *
     * <p>Warning: The type argument of the map is not
     * enforced by the API. The use of this method is an
     * unchecked assertion that the type argument is correct.
     *
     * @param mapping the mapping.
     * @return the appended sequence of checks.
     * @param <Q> the type of the array.
     */
    @SuppressWarnings("unchecked")
    public <Q extends Expression> BranchesIntermediary<T, R> isMap(final Function<? super MapExpression<@MqlUnchecked(TYPE_ARGUMENT) Q>, ? extends R> mapping) {
        Assertions.notNull("mapping", mapping);
        return is(v -> mqlEx(v).isDocumentOrMap(), v -> mapping.apply((MapExpression<Q>) v));
    }

    /**
     * A successful check for
     * {@linkplain Expressions#ofNull()} being the null value}
     * produces a value specified by the {@code mapping}.
     *
     * @param mapping the mapping.
     * @return the appended sequence of checks.
     */
    public BranchesIntermediary<T, R> isNull(final Function<? super Expression, ? extends R> mapping) {
        Assertions.notNull("mapping", mapping);
        return is(v -> mqlEx(v).isNull(), v -> mapping.apply(v));
    }

    /**
     * If no other check succeeds,
     * produces a value specified by the {@code mapping}.
     *
     * @param mapping the mapping.
     * @return the appended sequence of checks.
     */
    public BranchesTerminal<T, R> defaults(final Function<? super T, ? extends R> mapping) {
        Assertions.notNull("mapping", mapping);
        return this.withDefault(value -> mapping.apply(value));
    }

}
