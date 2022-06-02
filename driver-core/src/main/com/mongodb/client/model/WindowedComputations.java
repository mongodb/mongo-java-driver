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
package com.mongodb.client.model;

import com.mongodb.client.model.Windows.Bound;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonType;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static org.bson.assertions.Assertions.notNull;

/**
 * Builders for {@linkplain WindowedComputation windowed computations} used in the
 * {@link Aggregates#setWindowFields(Object, Bson, List) $setWindowFields} pipeline stage
 * of an aggregation pipeline. Each windowed computation is a triple:
 * <ul>
 *     <li>A window function. Some functions require documents in a window to be sorted
 *     (see {@code sortBy} in {@link Aggregates#setWindowFields(Object, Bson, List)}).</li>
 *     <li>An optional {@linkplain Window window}, a.k.a. frame.
 *     Specifying {@code null} window is equivalent to specifying an unbounded window,
 *     i.e., a window with both ends specified as {@link Bound#UNBOUNDED}.
 *     Some window functions, e.g., {@link #derivative(String, Object, Window)},
 *     require an explicit unbounded window instead of {@code null}.</li>
 *     <li>A path to an output field to be computed by the window function over the window.</li>
 * </ul>
 * A windowed computation is similar to an {@linkplain Accumulators accumulator} but does not result in folding documents constituting
 * the window into a single document.
 *
 * @mongodb.driver.manual meta/aggregation-quick-reference/#field-paths Field paths
 * @since 4.3
 * @mongodb.server.release 5.0
 */
public final class WindowedComputations {
    /**
     * Creates a windowed computation from a document field in situations when there is no builder method that better satisfies your needs.
     * This method cannot be used to validate the document field syntax.
     * <p>
     * <i>Example</i><br>
     * The following code creates two functionally equivalent windowed computations,
     * though they may not be {@linkplain #equals(Object) equal}.
     * <pre>{@code
     *  Window pastWeek = Windows.timeRange(-1, MongoTimeUnit.WEEK, Windows.Bound.CURRENT);
     *  WindowedComputation pastWeekExpenses1 = WindowedComputations.sum("pastWeekExpenses", "$expenses", pastWeek);
     *  WindowedComputation pastWeekExpenses2 = WindowedComputations.of(
     *          new BsonField("pastWeekExpenses", new Document("$sum", "$expenses")
     *                  .append("window", pastWeek.toBsonDocument())));
     * }</pre>
     *
     * @param windowedComputation A document field representing the required windowed computation.
     * @return The constructed windowed computation.
     */
    public static WindowedComputation of(final BsonField windowedComputation) {
        return new BsonFieldWindowedComputation(notNull("windowedComputation", windowedComputation));
    }

    /**
     * Builds a computation of the sum of the evaluation results of the {@code expression} over the {@code window}.
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-sum $sum
     */
    public static <TExpression> WindowedComputation sum(final String path, final TExpression expression, @Nullable final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        return simpleParameterWindowFunction(path, "$sum", expression, window);
    }

    /**
     * Builds a computation of the average of the evaluation results of the {@code expression} over the {@code window}.
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-avg $avg
     */
    public static <TExpression> WindowedComputation avg(final String path, final TExpression expression, @Nullable final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        return simpleParameterWindowFunction(path, "$avg", expression, window);
    }

    /**
     * Builds a computation of the sample standard deviation of the evaluation results of the {@code expression} over the {@code window}.
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-std-dev-samp $stdDevSamp
     */
    public static <TExpression> WindowedComputation stdDevSamp(final String path, final TExpression expression,
                                                               @Nullable final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        return simpleParameterWindowFunction(path, "$stdDevSamp", expression, window);
    }

    /**
     * Builds a computation of the population standard deviation of the evaluation results of the {@code expression}
     * over the {@code window}.
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-std-dev-pop $stdDevPop
     */
    public static <TExpression> WindowedComputation stdDevPop(final String path, final TExpression expression,
                                                              @Nullable final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        return simpleParameterWindowFunction(path, "$stdDevPop", expression, window);
    }

    /**
     * Builds a computation of the lowest of the evaluation results of the {@code expression} over the {@code window}.
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-min $min
     */
    public static <TExpression> WindowedComputation min(final String path, final TExpression expression, @Nullable final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        return simpleParameterWindowFunction(path, "$min", expression, window);
    }

    /**
     * Builds a computation of the highest of the evaluation results of the {@code expression} over the {@code window}.
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-max $max
     */
    public static <TExpression> WindowedComputation max(final String path, final TExpression expression, @Nullable final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        return simpleParameterWindowFunction(path, "$max", expression, window);
    }

    /**
     * Builds a computation of the number of documents in the {@code window}.
     *
     * @param path The output field path.
     * @param window The window.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-count $count
     */
    public static WindowedComputation count(final String path, @Nullable final Window window) {
        notNull("path", path);
        return simpleParameterWindowFunction(path, "$count", null, window);
    }

    /**
     * Builds a computation of the time derivative by subtracting the evaluation result of the {@code expression} against the last document
     * and the first document in the {@code window} and dividing it by the difference in the values of the
     * {@link Aggregates#setWindowFields(Object, Bson, List) sortBy} field of the respective documents.
     * Other documents in the {@code window} have no effect on the computation.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, List) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-derivative $derivative
     */
    public static <TExpression> WindowedComputation derivative(final String path, final TExpression expression,
                                                               final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        notNull("window", window);
        final Map<ParamName, Object> args = new HashMap<>(1);
        args.put(ParamName.INPUT, expression);
        return compoundParameterWindowFunction(path, "$derivative", args, window);
    }

    /**
     * Builds a computation of the time derivative by subtracting the evaluation result of the {@code expression} against the last document
     * and the first document in the {@code window} and dividing it by the difference in the BSON {@link BsonType#DATE_TIME Date}
     * values of the {@link Aggregates#setWindowFields(Object, Bson, List) sortBy} field of the respective documents.
     * Other documents in the {@code window} have no effect on the computation.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, List) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param unit The desired time unit for the divisor. Allowed values are:
     * {@link MongoTimeUnit#WEEK}, {@link MongoTimeUnit#DAY}, {@link MongoTimeUnit#HOUR}, {@link MongoTimeUnit#MINUTE},
     * {@link MongoTimeUnit#SECOND}, {@link MongoTimeUnit#MILLISECOND}.
     * @param <TExpression> The expression type.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-derivative $derivative
     */
    public static <TExpression> WindowedComputation timeDerivative(final String path, final TExpression expression, final Window window,
                                                                   final MongoTimeUnit unit) {
        notNull("path", path);
        notNull("expression", expression);
        notNull("window", window);
        notNull("unit", unit);
        isTrueArgument("unit must be either of WEEK, DAY, HOUR, MINUTE, SECOND, MILLISECOND", unit.fixed());
        final Map<ParamName, Object> args = new LinkedHashMap<>(2);
        args.put(ParamName.INPUT, expression);
        args.put(ParamName.UNIT, unit.value());
        return compoundParameterWindowFunction(path, "$derivative", args, window);
    }

    /**
     * Builds a computation of the approximate integral of a function that maps values of
     * the {@link Aggregates#setWindowFields(Object, Bson, List) sortBy} field to evaluation results of the {@code expression}
     * against the same document. The limits of integration match the {@code window} bounds.
     * The approximation is done by using the
     * <a href="https://www.khanacademy.org/math/ap-calculus-ab/ab-integration-new/ab-6-2/a/understanding-the-trapezoid-rule">
     * trapezoidal rule</a>.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, List) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-integral $integral
     */
    public static <TExpression> WindowedComputation integral(final String path, final TExpression expression, final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        notNull("window", window);
        final Map<ParamName, Object> args = new HashMap<>(1);
        args.put(ParamName.INPUT, expression);
        return compoundParameterWindowFunction(path, "$integral", args, window);
    }

    /**
     * Builds a computation of the approximate integral of a function that maps BSON {@link BsonType#DATE_TIME Date} values of
     * the {@link Aggregates#setWindowFields(Object, Bson, List) sortBy} field to evaluation results of the {@code expression}
     * against the same document. The limits of integration match the {@code window} bounds.
     * The approximation is done by using the trapezoidal rule.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, List) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param unit The desired time unit for the divisor. Allowed values are:
     * {@link MongoTimeUnit#WEEK}, {@link MongoTimeUnit#DAY}, {@link MongoTimeUnit#HOUR}, {@link MongoTimeUnit#MINUTE},
     * {@link MongoTimeUnit#SECOND}, {@link MongoTimeUnit#MILLISECOND}.
     * @param <TExpression> The expression type.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-integral $integral
     */
    public static <TExpression> WindowedComputation timeIntegral(final String path, final TExpression expression, final Window window,
                                                                 final MongoTimeUnit unit) {
        notNull("path", path);
        notNull("expression", expression);
        notNull("window", window);
        notNull("unit", unit);
        isTrueArgument("unit must be either of WEEK, DAY, HOUR, MINUTE, SECOND, MILLISECOND", unit.fixed());
        final Map<ParamName, Object> args = new LinkedHashMap<>(2);
        args.put(ParamName.INPUT, expression);
        args.put(ParamName.UNIT, unit.value());
        return compoundParameterWindowFunction(path, "$integral", args, window);
    }

    /**
     * Builds a computation of the sample covariance between the evaluation results of the two expressions over the {@code window}.
     *
     *
     * @param path The output field path.
     * @param expression1 The first expression.
     * @param expression2 The second expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-covariance-samp $covarianceSamp
     */
    public static <TExpression> WindowedComputation covarianceSamp(final String path, final TExpression expression1,
                                                                   final TExpression expression2, @Nullable final Window window) {
        notNull("path", path);
        notNull("expression1", expression1);
        notNull("expression2", expression2);
        List<TExpression> expressions = new ArrayList<>(2);
        expressions.add(expression1);
        expressions.add(expression2);
        return simpleParameterWindowFunction(path, "$covarianceSamp", expressions, window);
    }

    /**
     * Builds a computation of the population covariance between the evaluation results of the two expressions over the {@code window}.
     *
     * @param path The output field path.
     * @param expression1 The first expression.
     * @param expression2 The second expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-covariance-pop $covariancePop
     */
    public static <TExpression> WindowedComputation covariancePop(final String path, final TExpression expression1,
                                                                   final TExpression expression2, @Nullable final Window window) {
        notNull("path", path);
        notNull("expression1", expression1);
        notNull("expression2", expression2);
        List<TExpression> expressions = new ArrayList<>(2);
        expressions.add(expression1);
        expressions.add(expression2);
        return simpleParameterWindowFunction(path, "$covariancePop", expressions, window);
    }

    /**
     * Builds a computation of the exponential moving average of the evaluation results of the {@code expression} over a window
     * that includes {@code n} - 1 documents preceding the current document and the current document, with more weight on documents
     * closer to the current one.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, List) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param n Must be positive.
     * @param <TExpression> The expression type.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-exp-moving-avg $expMovingAvg
     */
    public static <TExpression> WindowedComputation expMovingAvg(final String path, final TExpression expression, final int n) {
        notNull("path", path);
        notNull("expression", expression);
        isTrueArgument("n > 0", n > 0);
        final Map<ParamName, Object> args = new LinkedHashMap<>(2);
        args.put(ParamName.INPUT, expression);
        args.put(ParamName.N, n);
        return compoundParameterWindowFunction(path, "$expMovingAvg", args, null);
    }

    /**
     * Builds a computation of the exponential moving average of the evaluation results of the {@code expression} over the half-bounded
     * window [{@link Bound#UNBOUNDED}, {@link Bound#CURRENT}], with {@code alpha} representing the degree of weighting decrease.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, List) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param alpha A parameter specifying how fast weighting decrease happens. A higher {@code alpha} discounts older observations faster.
     *              Must belong to the interval (0, 1).
     * @param <TExpression> The expression type.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-exp-moving-avg $expMovingAvg
     */
    public static <TExpression> WindowedComputation expMovingAvg(final String path, final TExpression expression, final double alpha) {
        notNull("path", path);
        notNull("expression", expression);
        isTrueArgument("alpha > 0", alpha > 0);
        isTrueArgument("alpha < 1", alpha < 1);
        final Map<ParamName, Object> args = new LinkedHashMap<>(2);
        args.put(ParamName.INPUT, expression);
        args.put(ParamName.ALPHA, alpha);
        return compoundParameterWindowFunction(path, "$expMovingAvg", args, null);
    }

    /**
     * Builds a computation that adds the evaluation results of the {@code expression} over the {@code window}
     * to a BSON {@link org.bson.BsonType#ARRAY Array}.
     * Order within the array is guaranteed if {@link Aggregates#setWindowFields(Object, Bson, List) sortBy} is specified.
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-push $push
     */
    public static <TExpression> WindowedComputation push(final String path, final TExpression expression, @Nullable final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        return simpleParameterWindowFunction(path, "$push", expression, window);
    }

    /**
     * Builds a computation that adds the evaluation results of the {@code expression} over the {@code window}
     * to a BSON {@link org.bson.BsonType#ARRAY Array} and excludes duplicates.
     * Order within the array is not specified.
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-add-to-set $addToSet
     */
    public static <TExpression> WindowedComputation addToSet(final String path, final TExpression expression,
                                                             @Nullable final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        return simpleParameterWindowFunction(path, "$addToSet", expression, window);
    }

    /**
     * Builds a computation of the evaluation result of the {@code expression} against the first document in the {@code window}.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, List) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-first $first
     */
    public static <TExpression> WindowedComputation first(final String path, final TExpression expression, @Nullable final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        return simpleParameterWindowFunction(path, "$first", expression, window);
    }

    /**
     * Builds a computation of the evaluation result of the {@code expression} against the last document in the {@code window}.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, List) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-last $last
     */
    public static <TExpression> WindowedComputation last(final String path, final TExpression expression, @Nullable final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        return simpleParameterWindowFunction(path, "$last", expression, window);
    }

    /**
     * Builds a computation of the evaluation result of the {@code expression} for the document whose position is shifted by the given
     * amount relative to the current document. If the shifted document is outside of the
     * {@linkplain Aggregates#setWindowFields(Object, Bson, List) partition} containing the current document,
     * then the {@code defaultExpression} is used instead of the {@code expression}.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, List) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param defaultExpression The default expression.
     *                          If {@code null}, then the default expression is evaluated to BSON {@link org.bson.BsonNull null}.
     *                          Must evaluate to a constant value.
     * @param by The shift specified similarly to {@linkplain Windows rules for window bounds}:
     *           <ul>
     *              <li>0 means the current document;</li>
     *              <li>a negative value refers to the document preceding the current one;</li>
     *              <li>a positive value refers to the document following the current one.</li>
     *           </ul>
     * @param <TExpression> The expression type.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-shift $shift
     */
    public static <TExpression> WindowedComputation shift(final String path, final TExpression expression,
                                                          @Nullable final TExpression defaultExpression, final int by) {
        notNull("path", path);
        notNull("expression", expression);
        final Map<ParamName, Object> args = new LinkedHashMap<>(3);
        args.put(ParamName.OUTPUT, expression);
        args.put(ParamName.BY, by);
        if (defaultExpression != null) {
            args.put(ParamName.DEFAULT, defaultExpression);
        }
        return compoundParameterWindowFunction(path, "$shift", args, null);
    }

    /**
     * Builds a computation of the order number of each document in its
     * {@linkplain Aggregates#setWindowFields(Object, Bson, List) partition}.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, List) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-document-number $documentNumber
     */
    public static WindowedComputation documentNumber(final String path) {
        notNull("path", path);
        return simpleParameterWindowFunction(path, "$documentNumber", null, null);
    }

    /**
     * Builds a computation of the rank of each document in its
     * {@linkplain Aggregates#setWindowFields(Object, Bson, List) partition}.
     * Documents with the same value(s) of the {@linkplain Aggregates#setWindowFields(Object, Bson, List) sortBy} fields result in
     * the same ranking and result in gaps in the returned ranks.
     * For example, a partition with the sequence [1, 3, 3, 5] representing the values of the single {@code sortBy} field
     * produces the following sequence of rank values: [1, 2, 2, 4].
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, List) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-rank $rank
     */
    public static WindowedComputation rank(final String path) {
        notNull("path", path);
        return simpleParameterWindowFunction(path, "$rank", null, null);
    }

    /**
     * Builds a computation of the dense rank of each document in its
     * {@linkplain Aggregates#setWindowFields(Object, Bson, List) partition}.
     * Documents with the same value(s) of the {@linkplain Aggregates#setWindowFields(Object, Bson, List) sortBy} fields result in
     * the same ranking but do not result in gaps in the returned ranks.
     * For example, a partition with the sequence [1, 3, 3, 5] representing the values of the single {@code sortBy} field
     * produces the following sequence of rank values: [1, 2, 2, 3].
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, List) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @return The constructed windowed computation.
     * @mongodb.driver.dochub core/window-functions-dense-rank $denseRank
     */
    public static WindowedComputation denseRank(final String path) {
        notNull("path", path);
        return simpleParameterWindowFunction(path, "$denseRank", null, null);
    }

    private static WindowedComputation simpleParameterWindowFunction(final String path, final String functionName,
                                                                     @Nullable final Object expression,
                                                                     @Nullable final Window window) {
        return new BsonFieldWindowedComputation(new BsonField(path,
                new SimpleParameterFunctionAndWindow(functionName, expression, window)));
    }

    private static WindowedComputation compoundParameterWindowFunction(final String path, final String functionName,
                                                                       final Map<ParamName, Object> args,
                                                                       @Nullable final Window window) {
        return new BsonFieldWindowedComputation(new BsonField(path,
                new CompoundParameterFunctionAndWindow(functionName, args, window)));
    }

    private WindowedComputations() {
        throw new UnsupportedOperationException();
    }

    private static final class BsonFieldWindowedComputation implements WindowedComputation {
        private final BsonField wrapped;

        BsonFieldWindowedComputation(final BsonField field) {
            wrapped = assertNotNull(field);
        }

        @Override
        public BsonField toBsonField() {
            return wrapped;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final BsonFieldWindowedComputation that = (BsonFieldWindowedComputation) o;
            return wrapped.equals(that.wrapped);
        }

        @Override
        public int hashCode() {
            return wrapped.hashCode();
        }

        @Override
        public String toString() {
            return wrapped.toString();
        }
    }

    /**
     * A combination of a window function and its window.
     */
    private abstract static class AbstractFunctionAndWindow implements Bson {
        private final String functionName;
        @Nullable
        private final Window window;

        AbstractFunctionAndWindow(final String functionName, @Nullable final Window window) {
            this.functionName = functionName;
            this.window = window;
        }

        final void writeWindow(final CodecRegistry codecRegistry, final BsonDocumentWriter writer) {
            if (window != null) {
                writer.writeName("window");
                BuildersHelper.encodeValue(writer, window, codecRegistry);
            }
        }

        final String functionName() {
            return functionName;
        }

        final Optional<Window> window() {
            return Optional.ofNullable(window);
        }
    }

    private static final class SimpleParameterFunctionAndWindow extends AbstractFunctionAndWindow {
        @Nullable
        private final Object expression;

        SimpleParameterFunctionAndWindow(final String functionName, @Nullable final Object expression, @Nullable final Window window) {
            super(functionName, window);
            this.expression = expression;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
            writer.writeStartDocument();
            writer.writeName(functionName());
            if (expression == null) {
                writer.writeStartDocument();
                writer.writeEndDocument();
            } else {
                BuildersHelper.encodeValue(writer, expression, codecRegistry);
            }
            writeWindow(codecRegistry, writer);
            writer.writeEndDocument();
            return writer.getDocument();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final SimpleParameterFunctionAndWindow that = (SimpleParameterFunctionAndWindow) o;
            return functionName().equals(that.functionName()) && Objects.equals(expression, that.expression)
                    && window().equals(that.window());
        }

        @Override
        public int hashCode() {
            return Objects.hash(functionName(), expression, window());
        }

        @Override
        public String toString() {
            return "WindowFunction{"
                    + "name='" + functionName() + '\''
                    + ", expression=" + expression
                    + ", window=" + window()
                    + '}';
        }
    }

    private static final class CompoundParameterFunctionAndWindow extends AbstractFunctionAndWindow {
        private final Map<WindowedComputations.ParamName, Object> args;

        CompoundParameterFunctionAndWindow(final String functionName, final Map<WindowedComputations.ParamName, Object> args,
                                           @Nullable final Window window) {
            super(functionName, window);
            this.args = args;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
            writer.writeStartDocument();
            writer.writeName(functionName());
            writer.writeStartDocument();
            args.forEach((paramName, paramValue) -> {
                writer.writeName(paramName.value());
                BuildersHelper.encodeValue(writer, paramValue, codecRegistry);
            });
            writer.writeEndDocument();
            writeWindow(codecRegistry, writer);
            writer.writeEndDocument();
            return writer.getDocument();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final CompoundParameterFunctionAndWindow that = (CompoundParameterFunctionAndWindow) o;
            return functionName().equals(that.functionName()) && Objects.equals(args, that.args) && window().equals(that.window());
        }

        @Override
        public int hashCode() {
            return Objects.hash(functionName(), args, window());
        }

        @Override
        public String toString() {
            return "WindowFunction{"
                    + "name='" + functionName() + '\''
                    + ", args=" + args
                    + ", window=" + window()
                    + '}';
        }
    }

    private enum ParamName {
        INPUT("input"),
        UNIT("unit"),
        N("N"),
        ALPHA("alpha"),
        OUTPUT("output"),
        BY("by"),
        DEFAULT("default");

        private final String value;

        ParamName(final String value) {
            this.value = value;
        }

        String value() {
            return value;
        }
    }
}
