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

package com.mongodb.client.model.mql;

import com.mongodb.annotations.Beta;
import com.mongodb.assertions.Assertions;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.mongodb.client.model.mql.MqlUnchecked.Unchecked.TYPE_ARGUMENT;

/**
 * See {@link Branches}.
 *
 * @param <T> the type of the values that may be checked.
 * @param <R> the type of the value produced.
 * @since 4.9.0
 */
@Beta(Beta.Reason.CLIENT)
public final class BranchesIntermediary<T extends MqlValue, R extends MqlValue> extends BranchesTerminal<T, R> {
    BranchesIntermediary(final List<Function<T, SwitchCase<R>>> branches) {
        super(branches, null);
    }

    private BranchesIntermediary<T, R> with(final Function<T, SwitchCase<R>> switchCase) {
        List<Function<T, SwitchCase<R>>> v = new ArrayList<>(this.getBranches());
        v.add(switchCase);
        return new BranchesIntermediary<>(v);
    }

    private static <T extends MqlValue> MqlExpression<?> mqlEx(final T value) {
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
    public BranchesIntermediary<T, R> is(final Function<? super T, MqlBoolean> predicate, final Function<? super T, ? extends R> mapping) {
        Assertions.notNull("predicate", predicate);
        Assertions.notNull("mapping", mapping);
        return this.with(value -> new SwitchCase<>(predicate.apply(value), mapping.apply(value)));
    }

    // eq lt lte

    /**
     * A successful check for {@linkplain MqlValue#eq equality}
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
     * {@linkplain MqlValue#lt less than}
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
     * {@linkplain MqlValue#lte less than or equal to}
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
     * {@linkplain MqlValue#isBooleanOr(MqlBoolean) being a boolean}
     * produces a value specified by the {@code mapping}.
     *
     * @param mapping the mapping.
     * @return the appended sequence of checks.
     */
    public BranchesIntermediary<T, R> isBoolean(final Function<? super MqlBoolean, ? extends R> mapping) {
        Assertions.notNull("mapping", mapping);
        return is(v -> mqlEx(v).isBoolean(), v -> mapping.apply((MqlBoolean) v));
    }

    /**
     * A successful check for
     * {@linkplain MqlValue#isNumberOr(MqlNumber) being a number}
     * produces a value specified by the {@code mapping}.
     *
     * @mongodb.server.release 4.4
     * @param mapping the mapping.
     * @return the appended sequence of checks.
     */
    public BranchesIntermediary<T, R> isNumber(final Function<? super MqlNumber, ? extends R> mapping) {
        Assertions.notNull("mapping", mapping);
        return is(v -> mqlEx(v).isNumber(), v -> mapping.apply((MqlNumber) v));
    }

    /**
     * A successful check for
     * {@linkplain MqlValue#isIntegerOr(MqlInteger) being an integer}
     * produces a value specified by the {@code mapping}.
     *
     * @mongodb.server.release 4.4
     * @param mapping the mapping.
     * @return the appended sequence of checks.
     */
    public BranchesIntermediary<T, R> isInteger(final Function<? super MqlInteger, ? extends R> mapping) {
        Assertions.notNull("mapping", mapping);
        return is(v -> mqlEx(v).isInteger(), v -> mapping.apply((MqlInteger) v));
    }

    /**
     * A successful check for
     * {@linkplain MqlValue#isStringOr(MqlString) being a string}
     * produces a value specified by the {@code mapping}.
     *
     * @param mapping the mapping.
     * @return the appended sequence of checks.
     */
    public BranchesIntermediary<T, R> isString(final Function<? super MqlString, ? extends R> mapping) {
        Assertions.notNull("mapping", mapping);
        return is(v -> mqlEx(v).isString(), v -> mapping.apply((MqlString) v));
    }

    /**
     * A successful check for
     * {@linkplain MqlValue#isDateOr(MqlDate) being a date}
     * produces a value specified by the {@code mapping}.
     *
     * @param mapping the mapping.
     * @return the appended sequence of checks.
     */
    public BranchesIntermediary<T, R> isDate(final Function<? super MqlDate, ? extends R> mapping) {
        Assertions.notNull("mapping", mapping);
        return is(v -> mqlEx(v).isDate(), v -> mapping.apply((MqlDate) v));
    }

    /**
     * A successful check for
     * {@linkplain MqlValue#isArrayOr(MqlArray) being an array}
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
    public <Q extends MqlValue> BranchesIntermediary<T, R> isArray(final Function<? super MqlArray<@MqlUnchecked(TYPE_ARGUMENT) Q>, ? extends R> mapping) {
        Assertions.notNull("mapping", mapping);
        return is(v -> mqlEx(v).isArray(), v -> mapping.apply((MqlArray<Q>) v));
    }

    /**
     * A successful check for
     * {@linkplain MqlValue#isDocumentOr(MqlDocument) being a document}
     * (or document-like value, see
     * {@link MqlMap} and {@link MqlEntry})
     * produces a value specified by the {@code mapping}.
     *
     * @param mapping the mapping.
     * @return the appended sequence of checks.
     */
    public BranchesIntermediary<T, R> isDocument(final Function<? super MqlDocument, ? extends R> mapping) {
        Assertions.notNull("mapping", mapping);
        return is(v -> mqlEx(v).isDocumentOrMap(), v -> mapping.apply((MqlDocument) v));
    }

    /**
     * A successful check for
     * {@linkplain MqlValue#isMapOr(MqlMap) being a map}
     * (or map-like value, see
     * {@link MqlDocument} and {@link MqlEntry})
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
    public <Q extends MqlValue> BranchesIntermediary<T, R> isMap(final Function<? super MqlMap<@MqlUnchecked(TYPE_ARGUMENT) Q>, ? extends R> mapping) {
        Assertions.notNull("mapping", mapping);
        return is(v -> mqlEx(v).isDocumentOrMap(), v -> mapping.apply((MqlMap<Q>) v));
    }

    /**
     * A successful check for
     * {@linkplain MqlValues#ofNull()} being the null value}
     * produces a value specified by the {@code mapping}.
     *
     * @param mapping the mapping.
     * @return the appended sequence of checks.
     */
    public BranchesIntermediary<T, R> isNull(final Function<? super MqlValue, ? extends R> mapping) {
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
