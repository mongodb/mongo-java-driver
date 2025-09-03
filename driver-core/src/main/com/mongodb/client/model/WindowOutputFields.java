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
 * Builders for {@linkplain WindowOutputField window output fields} used in the
 * {@link Aggregates#setWindowFields(Object, Bson, Iterable) $setWindowFields} pipeline stage
 * of an aggregation pipeline. Each windowed computation is a triple:
 * <ul>
 *     <li>A window function. Some functions require documents in a window to be sorted
 *     (see {@code sortBy} in {@link Aggregates#setWindowFields(Object, Bson, Iterable)}).</li>
 *     <li>An optional {@linkplain Window window}, a.k.a. frame.
 *     Specifying {@code null} window is equivalent to specifying an unbounded window,
 *     i.e., a window with both ends specified as {@link Bound#UNBOUNDED}.
 *     Some window functions, e.g., {@link #derivative(String, Object, Window)},
 *     require an explicit unbounded window instead of {@code null}.</li>
 *     <li>A path to an output field to be computed by the window function over the window.</li>
 * </ul>
 * A window output field is similar to an {@linkplain Accumulators accumulator} but does not result in folding documents constituting
 * the window into a single document.
 *
 * @mongodb.driver.manual meta/aggregation-quick-reference/#field-paths Field paths
 * @since 4.3
 * @mongodb.server.release 5.0
 */
public final class WindowOutputFields {
    /**
     * Creates a windowed output field from a document field in situations when there is no builder method that better satisfies your needs.
     * This method cannot be used to validate the document field syntax.
     * <p>
     * <i>Example</i><br>
     * The following code creates two functionally equivalent window output fields,
     * though they may not be {@linkplain #equals(Object) equal}.
     * <pre>{@code
     *  Window pastWeek = Windows.timeRange(-1, MongoTimeUnit.WEEK, Windows.Bound.CURRENT);
     *  WindowOutputField pastWeekExpenses1 = WindowOutputFields.sum("pastWeekExpenses", "$expenses", pastWeek);
     *  WindowOutputField pastWeekExpenses2 = WindowOutputFields.of(
     *          new BsonField("pastWeekExpenses", new Document("$sum", "$expenses")
     *                  .append("window", pastWeek.toBsonDocument())));
     * }</pre>
     *
     * @param windowOutputField A document field representing the required windowed output field.
     * @return The constructed windowed output field.
     */
    public static WindowOutputField of(final BsonField windowOutputField) {
        return new BsonFieldWindowOutputField(notNull("windowOutputField", windowOutputField));
    }

    /**
     * Builds a window output field of the sum of the evaluation results of the {@code expression} over the {@code window}.
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed output field.
     * @mongodb.driver.dochub core/window-functions-sum $sum
     */
    public static <TExpression> WindowOutputField sum(final String path, final TExpression expression, @Nullable final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        return simpleParameterWindowFunction(path, "$sum", expression, window);
    }

    /**
     * Builds a window output field of the average of the evaluation results of the {@code expression} over the {@code window}.
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed output field.
     * @mongodb.driver.dochub core/window-functions-avg $avg
     */
    public static <TExpression> WindowOutputField avg(final String path, final TExpression expression, @Nullable final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        return simpleParameterWindowFunction(path, "$avg", expression, window);
    }

    /**
     * Builds a window output field of percentiles of the evaluation results of the {@code inExpression}
     * over documents in the specified {@code window}. The {@code pExpression} parameter represents an array of
     * percentiles of interest, with each element being a numeric value between 0.0 and 1.0 (inclusive).
     *
     * @param path The output field path.
     * @param inExpression The input expression.
     * @param pExpression The expression representing a percentiles of interest.
     * @param method The method to be used for computing the percentiles.
     * @param window The window.
     * @param <InExpression> The type of the input expression.
     * @param <PExpression> The type of the percentile expression.
     * @return The constructed windowed output field.
     * @mongodb.driver.manual reference/operator/aggregation/percentile/ $percentile
     * @since 4.10
     * @mongodb.server.release 7.0
     */
    public static <InExpression, PExpression> WindowOutputField percentile(final String path, final InExpression inExpression,
                                                                           final PExpression pExpression, final QuantileMethod method,
                                                                           @Nullable final Window window) {
        notNull("path", path);
        notNull("inExpression", inExpression);
        notNull("pExpression", pExpression);
        notNull("method", method);
        Map<ParamName, Object> args = new LinkedHashMap<>(3);
        args.put(ParamName.INPUT, inExpression);
        args.put(ParamName.P_LOWERCASE, pExpression);
        args.put(ParamName.METHOD, method.toBsonValue());
        return compoundParameterWindowFunction(path, "$percentile", args, window);
    }

    /**
     * Builds a window output field representing the median value of the evaluation results of the {@code inExpression}
     * over documents in the specified {@code window}.
     *
     * @param path The output field path.
     * @param inExpression The input expression.
     * @param method The method to be used for computing the median.
     * @param window The window.
     * @param <InExpression> The type of the input expression.
     * @return The constructed windowed output field.
     * @mongodb.driver.manual reference/operator/aggregation/median/ $median
     * @since 4.10
     * @mongodb.server.release 7.0
     */
    public static <InExpression> WindowOutputField median(final String path, final InExpression inExpression,
                                                          final QuantileMethod method,
                                                          @Nullable final Window window) {
        notNull("path", path);
        notNull("inExpression", inExpression);
        notNull("method", method);
        Map<ParamName, Object> args = new LinkedHashMap<>(2);
        args.put(ParamName.INPUT, inExpression);
        args.put(ParamName.METHOD, method.toBsonValue());
        return compoundParameterWindowFunction(path, "$median", args, window);
    }

    /**
     * Builds a window output field of the sample standard deviation of the evaluation results of the {@code expression} over the
     * {@code window}.
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed output field.
     * @mongodb.driver.dochub core/window-functions-std-dev-samp $stdDevSamp
     */
    public static <TExpression> WindowOutputField stdDevSamp(final String path, final TExpression expression,
                                                               @Nullable final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        return simpleParameterWindowFunction(path, "$stdDevSamp", expression, window);
    }

    /**
     * Builds a window output field of the population standard deviation of the evaluation results of the {@code expression}
     * over the {@code window}.
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed output field.
     * @mongodb.driver.dochub core/window-functions-std-dev-pop $stdDevPop
     */
    public static <TExpression> WindowOutputField stdDevPop(final String path, final TExpression expression,
                                                              @Nullable final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        return simpleParameterWindowFunction(path, "$stdDevPop", expression, window);
    }

    /**
     * Builds a window output field of the smallest of the evaluation results of the {@code expression} over the {@code window}.
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed output field.
     * @mongodb.driver.manual reference/operator/aggregation/min/ $min
     */
    public static <TExpression> WindowOutputField min(final String path, final TExpression expression, @Nullable final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        return simpleParameterWindowFunction(path, "$min", expression, window);
    }

    /**
     * Builds a window output field of a BSON {@link org.bson.BsonType#ARRAY Array}
     * of {@code N} smallest evaluation results of the {@code inExpression} over the {@code window},
     * where {@code N} is the positive integral value of the {@code nExpression}.
     *
     * @param path The output field path.
     * @param inExpression The input expression.
     * @param nExpression The expression limiting the number of produced values.
     * @param window The window.
     * @param <InExpression> The type of the input expression.
     * @param <NExpression> The type of the limiting expression.
     * @return The constructed windowed output field.
     * @mongodb.driver.manual reference/operator/aggregation/minN/ $minN
     * @since 4.7
     * @mongodb.server.release 5.2
     */
    public static <InExpression, NExpression> WindowOutputField minN(
            final String path, final InExpression inExpression, final NExpression nExpression, @Nullable final Window window) {
        notNull("path", path);
        notNull("inExpression", inExpression);
        notNull("nExpression", nExpression);
        Map<ParamName, Object> args = new LinkedHashMap<>(3);
        args.put(ParamName.INPUT, inExpression);
        args.put(ParamName.N_LOWERCASE, nExpression);
        return compoundParameterWindowFunction(path, "$minN", args, window);
    }

    /**
     * Builds a window output field of the largest of the evaluation results of the {@code expression} over the {@code window}.
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed output field.
     * @mongodb.driver.manual reference/operator/aggregation/max/ $max
     */
    public static <TExpression> WindowOutputField max(final String path, final TExpression expression, @Nullable final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        return simpleParameterWindowFunction(path, "$max", expression, window);
    }

    /**
     * Builds a window output field of a BSON {@link org.bson.BsonType#ARRAY Array}
     * of {@code N} largest evaluation results of the {@code inExpression} over the {@code window},
     * where {@code N} is the positive integral value of the {@code nExpression}.
     *
     * @param path The output field path.
     * @param inExpression The input expression.
     * @param nExpression The expression limiting the number of produced values.
     * @param window The window.
     * @param <InExpression> The type of the input expression.
     * @param <NExpression> The type of the limiting expression.
     * @return The constructed windowed output field.
     * @mongodb.driver.manual reference/operator/aggregation/maxN/ $maxN
     * @since 4.7
     * @mongodb.server.release 5.2
     */
    public static <InExpression, NExpression> WindowOutputField maxN(
            final String path, final InExpression inExpression, final NExpression nExpression, @Nullable final Window window) {
        notNull("path", path);
        notNull("inExpression", inExpression);
        notNull("nExpression", nExpression);
        Map<ParamName, Object> args = new LinkedHashMap<>(3);
        args.put(ParamName.INPUT, inExpression);
        args.put(ParamName.N_LOWERCASE, nExpression);
        return compoundParameterWindowFunction(path, "$maxN", args, window);
    }

    /**
     * Builds a window output field of the number of documents in the {@code window}.
     *
     * @param path The output field path.
     * @param window The window.
     * @return The constructed windowed output field.
     * @mongodb.driver.dochub core/window-functions-count $count
     */
    public static WindowOutputField count(final String path, @Nullable final Window window) {
        notNull("path", path);
        return simpleParameterWindowFunction(path, "$count", null, window);
    }

    /**
     * Builds a window output field of the time derivative by subtracting the evaluation result of the {@code expression} against the last document
     * and the first document in the {@code window} and dividing it by the difference in the values of the
     * {@link Aggregates#setWindowFields(Object, Bson, Iterable) sortBy} field of the respective documents.
     * Other documents in the {@code window} have no effect on the computation.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, Iterable) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed output field.
     * @mongodb.driver.dochub core/window-functions-derivative $derivative
     */
    public static <TExpression> WindowOutputField derivative(final String path, final TExpression expression,
                                                               final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        notNull("window", window);
        Map<ParamName, Object> args = new HashMap<>(1);
        args.put(ParamName.INPUT, expression);
        return compoundParameterWindowFunction(path, "$derivative", args, window);
    }

    /**
     * Builds a window output field of the time derivative by subtracting the evaluation result of the {@code expression} against the last
     * document and the first document in the {@code window} and dividing it by the difference in the BSON {@link BsonType#DATE_TIME Date}
     * values of the {@link Aggregates#setWindowFields(Object, Bson, Iterable) sortBy} field of the respective documents.
     * Other documents in the {@code window} have no effect on the computation.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, Iterable) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param unit The desired time unit for the divisor. Allowed values are:
     * {@link MongoTimeUnit#WEEK}, {@link MongoTimeUnit#DAY}, {@link MongoTimeUnit#HOUR}, {@link MongoTimeUnit#MINUTE},
     * {@link MongoTimeUnit#SECOND}, {@link MongoTimeUnit#MILLISECOND}.
     * @param <TExpression> The expression type.
     * @return The constructed windowed output field.
     * @mongodb.driver.dochub core/window-functions-derivative $derivative
     */
    public static <TExpression> WindowOutputField timeDerivative(final String path, final TExpression expression, final Window window,
                                                                   final MongoTimeUnit unit) {
        notNull("path", path);
        notNull("expression", expression);
        notNull("window", window);
        notNull("unit", unit);
        isTrueArgument("unit must be either of WEEK, DAY, HOUR, MINUTE, SECOND, MILLISECOND", unit.fixed());
        Map<ParamName, Object> args = new LinkedHashMap<>(2);
        args.put(ParamName.INPUT, expression);
        args.put(ParamName.UNIT, unit.value());
        return compoundParameterWindowFunction(path, "$derivative", args, window);
    }

    /**
     * Builds a window output field of the approximate integral of a function that maps values of
     * the {@link Aggregates#setWindowFields(Object, Bson, Iterable) sortBy} field to evaluation results of the {@code expression}
     * against the same document. The limits of integration match the {@code window} bounds.
     * The approximation is done by using the
     * <a href="https://www.khanacademy.org/math/ap-calculus-ab/ab-integration-new/ab-6-2/a/understanding-the-trapezoid-rule">
     * trapezoidal rule</a>.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, Iterable) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed output field.
     * @mongodb.driver.dochub core/window-functions-integral $integral
     */
    public static <TExpression> WindowOutputField integral(final String path, final TExpression expression, final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        notNull("window", window);
        Map<ParamName, Object> args = new HashMap<>(1);
        args.put(ParamName.INPUT, expression);
        return compoundParameterWindowFunction(path, "$integral", args, window);
    }

    /**
     * Builds a window output field of the approximate integral of a function that maps BSON {@link BsonType#DATE_TIME Date} values of
     * the {@link Aggregates#setWindowFields(Object, Bson, Iterable) sortBy} field to evaluation results of the {@code expression}
     * against the same document. The limits of integration match the {@code window} bounds.
     * The approximation is done by using the trapezoidal rule.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, Iterable) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param unit The desired time unit for the divisor. Allowed values are:
     * {@link MongoTimeUnit#WEEK}, {@link MongoTimeUnit#DAY}, {@link MongoTimeUnit#HOUR}, {@link MongoTimeUnit#MINUTE},
     * {@link MongoTimeUnit#SECOND}, {@link MongoTimeUnit#MILLISECOND}.
     * @param <TExpression> The expression type.
     * @return The constructed windowed output field.
     * @mongodb.driver.dochub core/window-functions-integral $integral
     */
    public static <TExpression> WindowOutputField timeIntegral(final String path, final TExpression expression, final Window window,
                                                                 final MongoTimeUnit unit) {
        notNull("path", path);
        notNull("expression", expression);
        notNull("window", window);
        notNull("unit", unit);
        isTrueArgument("unit must be either of WEEK, DAY, HOUR, MINUTE, SECOND, MILLISECOND", unit.fixed());
        Map<ParamName, Object> args = new LinkedHashMap<>(2);
        args.put(ParamName.INPUT, expression);
        args.put(ParamName.UNIT, unit.value());
        return compoundParameterWindowFunction(path, "$integral", args, window);
    }

    /**
     * Builds a window output field of the sample covariance between the evaluation results of the two expressions over the {@code window}.
     *
     * @param path The output field path.
     * @param expression1 The first expression.
     * @param expression2 The second expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed output field.
     * @mongodb.driver.dochub core/window-functions-covariance-samp $covarianceSamp
     */
    public static <TExpression> WindowOutputField covarianceSamp(final String path, final TExpression expression1,
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
     * Builds a window output field of the population covariance between the evaluation results of the two expressions over the {@code window}.
     *
     * @param path The output field path.
     * @param expression1 The first expression.
     * @param expression2 The second expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed output field.
     * @mongodb.driver.dochub core/window-functions-covariance-pop $covariancePop
     */
    public static <TExpression> WindowOutputField covariancePop(final String path, final TExpression expression1,
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
     * Builds a window output field of the exponential moving average of the evaluation results of the {@code expression} over a window
     * that includes {@code n} - 1 documents preceding the current document and the current document, with more weight on documents
     * closer to the current one.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, Iterable) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param n Must be positive.
     * @param <TExpression> The expression type.
     * @return The constructed windowed output field.
     * @mongodb.driver.dochub core/window-functions-exp-moving-avg $expMovingAvg
     */
    public static <TExpression> WindowOutputField expMovingAvg(final String path, final TExpression expression, final int n) {
        notNull("path", path);
        notNull("expression", expression);
        isTrueArgument("n > 0", n > 0);
        Map<ParamName, Object> args = new LinkedHashMap<>(2);
        args.put(ParamName.INPUT, expression);
        args.put(ParamName.N_UPPERCASE, n);
        return compoundParameterWindowFunction(path, "$expMovingAvg", args, null);
    }

    /**
     * Builds a window output field of the exponential moving average of the evaluation results of the {@code expression} over the half-bounded
     * window [{@link Bound#UNBOUNDED}, {@link Bound#CURRENT}], with {@code alpha} representing the degree of weighting decrease.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, Iterable) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param alpha A parameter specifying how fast weighting decrease happens. A higher {@code alpha} discounts older observations faster.
     *              Must belong to the interval (0, 1).
     * @param <TExpression> The expression type.
     * @return The constructed windowed output field.
     * @mongodb.driver.dochub core/window-functions-exp-moving-avg $expMovingAvg
     */
    public static <TExpression> WindowOutputField expMovingAvg(final String path, final TExpression expression, final double alpha) {
        notNull("path", path);
        notNull("expression", expression);
        isTrueArgument("alpha > 0", alpha > 0);
        isTrueArgument("alpha < 1", alpha < 1);
        Map<ParamName, Object> args = new LinkedHashMap<>(2);
        args.put(ParamName.INPUT, expression);
        args.put(ParamName.ALPHA, alpha);
        return compoundParameterWindowFunction(path, "$expMovingAvg", args, null);
    }

    /**
     * Builds a window output field that adds the evaluation results of the {@code expression} over the {@code window}
     * to a BSON {@link org.bson.BsonType#ARRAY Array}.
     * Order within the array is guaranteed if {@link Aggregates#setWindowFields(Object, Bson, Iterable) sortBy} is specified.
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed output field.
     * @mongodb.driver.dochub core/window-functions-push $push
     */
    public static <TExpression> WindowOutputField push(final String path, final TExpression expression, @Nullable final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        return simpleParameterWindowFunction(path, "$push", expression, window);
    }

    /**
     * Builds a window output field that adds the evaluation results of the {@code expression} over the {@code window}
     * to a BSON {@link org.bson.BsonType#ARRAY Array} and excludes duplicates.
     * Order within the array is not specified.
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed output field.
     * @mongodb.driver.dochub core/window-functions-add-to-set $addToSet
     */
    public static <TExpression> WindowOutputField addToSet(final String path, final TExpression expression,
                                                             @Nullable final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        return simpleParameterWindowFunction(path, "$addToSet", expression, window);
    }

    /**
     * Builds a window output field of the evaluation result of the {@code expression} against the first document in the {@code window}.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, Iterable) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed output field.
     * @mongodb.driver.manual reference/operator/aggregation/first/ $first
     */
    public static <TExpression> WindowOutputField first(final String path, final TExpression expression, @Nullable final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        return simpleParameterWindowFunction(path, "$first", expression, window);
    }

    /**
     * Builds a window output field of a BSON {@link org.bson.BsonType#ARRAY Array}
     * of evaluation results of the {@code inExpression} against the first {@code N} documents in the {@code window},
     * where {@code N} is the positive integral value of the {@code nExpression}.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, Iterable) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @param inExpression The input expression.
     * @param nExpression The expression limiting the number of produced values.
     * @param window The window.
     * @param <InExpression> The type of the input expression.
     * @param <NExpression> The type of the limiting expression.
     * @return The constructed windowed output field.
     * @mongodb.driver.manual reference/operator/aggregation/firstN/ $firstN
     * @since 4.7
     * @mongodb.server.release 5.2
     */
    public static <InExpression, NExpression> WindowOutputField firstN(
            final String path, final InExpression inExpression, final NExpression nExpression, @Nullable final Window window) {
        notNull("path", path);
        notNull("inExpression", inExpression);
        notNull("nExpression", nExpression);
        Map<ParamName, Object> args = new LinkedHashMap<>(3);
        args.put(ParamName.INPUT, inExpression);
        args.put(ParamName.N_LOWERCASE, nExpression);
        return compoundParameterWindowFunction(path, "$firstN", args, window);
    }

    /**
     * Builds a window output field of the evaluation result of the {@code outExpression} against the top document in the {@code window}
     * sorted according to the provided {@code sortBy} specification.
     *
     * @param path The output field path.
     * @param sortBy The {@linkplain Sorts sortBy specification}. The syntax is identical to the one expected by {@link Aggregates#sort(Bson)}.
     * @param outExpression The input expression.
     * @param window The window.
     * @param <OutExpression> The type of the output expression.
     * @return The constructed windowed output field.
     * @mongodb.driver.manual reference/operator/aggregation/top/ $top
     * @since 4.7
     * @mongodb.server.release 5.2
     */
    public static <OutExpression> WindowOutputField top(
            final String path, final Bson sortBy, final OutExpression outExpression, @Nullable final Window window) {
        notNull("path", path);
        notNull("sortBy", sortBy);
        notNull("outExpression", outExpression);
        Map<ParamName, Object> args = new LinkedHashMap<>(3);
        args.put(ParamName.SORT_BY, sortBy);
        args.put(ParamName.OUTPUT, outExpression);
        return compoundParameterWindowFunction(path, "$top", args, window);
    }

    /**
     * Builds a window output field of a BSON {@link org.bson.BsonType#ARRAY Array}
     * of evaluation results of the {@code outExpression} against the top {@code N} documents in the {@code window}
     * sorted according to the provided {@code sortBy} specification,
     * where {@code N} is the positive integral value of the {@code nExpression}.
     *
     * @param path The output field path.
     * @param sortBy The {@linkplain Sorts sortBy specification}. The syntax is identical to the one expected by {@link Aggregates#sort(Bson)}.
     * @param outExpression The input expression.
     * @param nExpression The expression limiting the number of produced values.
     * @param window The window.
     * @param <OutExpression> The type of the output expression.
     * @param <NExpression> The type of the limiting expression.
     * @return The constructed windowed output field.
     * @mongodb.driver.manual reference/operator/aggregation/topN/ $topN
     * @since 4.7
     * @mongodb.server.release 5.2
     */
    public static <OutExpression, NExpression> WindowOutputField topN(
            final String path, final Bson sortBy, final OutExpression outExpression, final NExpression nExpression, @Nullable final Window window) {
        notNull("path", path);
        notNull("sortBy", sortBy);
        notNull("outExpression", outExpression);
        notNull("nExpression", nExpression);
        Map<ParamName, Object> args = new LinkedHashMap<>(3);
        args.put(ParamName.SORT_BY, sortBy);
        args.put(ParamName.OUTPUT, outExpression);
        args.put(ParamName.N_LOWERCASE, nExpression);
        return compoundParameterWindowFunction(path, "$topN", args, window);
    }

    /**
     * Builds a window output field of the evaluation result of the {@code expression} against the last document in the {@code window}.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, Iterable) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param window The window.
     * @param <TExpression> The expression type.
     * @return The constructed windowed output field.
     * @mongodb.driver.manual reference/operator/aggregation/last/ $last
     */
    public static <TExpression> WindowOutputField last(final String path, final TExpression expression, @Nullable final Window window) {
        notNull("path", path);
        notNull("expression", expression);
        return simpleParameterWindowFunction(path, "$last", expression, window);
    }

    /**
     * Builds a window output field of a BSON {@link org.bson.BsonType#ARRAY Array}
     * of evaluation results of the {@code inExpression} against the last {@code N} documents in the {@code window},
     * where {@code N} is the positive integral value of the {@code nExpression}.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, Iterable) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @param inExpression The input expression.
     * @param nExpression The expression limiting the number of produced values.
     * @param window The window.
     * @param <InExpression> The type of the input expression.
     * @param <NExpression> The type of the limiting expression.
     * @return The constructed windowed output field.
     * @mongodb.driver.manual reference/operator/aggregation/lastN/ $lastN
     * @since 4.7
     * @mongodb.server.release 5.2
     */
    public static <InExpression, NExpression> WindowOutputField lastN(
            final String path, final InExpression inExpression, final NExpression nExpression, @Nullable final Window window) {
        notNull("path", path);
        notNull("inExpression", inExpression);
        notNull("nExpression", nExpression);
        Map<ParamName, Object> args = new LinkedHashMap<>(3);
        args.put(ParamName.INPUT, inExpression);
        args.put(ParamName.N_LOWERCASE, nExpression);
        return compoundParameterWindowFunction(path, "$lastN", args, window);
    }

    /**
     * Builds a window output field of the evaluation result of the {@code outExpression} against the bottom document in the {@code window}
     * sorted according to the provided {@code sortBy} specification.
     *
     * @param path The output field path.
     * @param sortBy The {@linkplain Sorts sortBy specification}. The syntax is identical to the one expected by {@link Aggregates#sort(Bson)}.
     * @param outExpression The input expression.
     * @param window The window.
     * @param <OutExpression> The type of the output expression.
     * @return The constructed windowed output field.
     * @mongodb.driver.manual reference/operator/aggregation/bottom/ $bottom
     * @since 4.7
     * @mongodb.server.release 5.2
     */
    public static <OutExpression> WindowOutputField bottom(
            final String path, final Bson sortBy, final OutExpression outExpression, @Nullable final Window window) {
        notNull("path", path);
        notNull("sortBy", sortBy);
        notNull("outExpression", outExpression);
        Map<ParamName, Object> args = new LinkedHashMap<>(3);
        args.put(ParamName.SORT_BY, sortBy);
        args.put(ParamName.OUTPUT, outExpression);
        return compoundParameterWindowFunction(path, "$bottom", args, window);
    }

    /**
     * Builds a window output field of a BSON {@link org.bson.BsonType#ARRAY Array}
     * of evaluation results of the {@code outExpression} against the bottom {@code N} documents in the {@code window}
     * sorted according to the provided {@code sortBy} specification,
     * where {@code N} is the positive integral value of the {@code nExpression}.
     *
     * @param path The output field path.
     * @param sortBy The {@linkplain Sorts sortBy specification}. The syntax is identical to the one expected by {@link Aggregates#sort(Bson)}.
     * @param outExpression The input expression.
     * @param nExpression The expression limiting the number of produced values.
     * @param window The window.
     * @param <OutExpression> The type of the output expression.
     * @param <NExpression> The type of the limiting expression.
     * @return The constructed windowed output field.
     * @mongodb.driver.manual reference/operator/aggregation/bottomN/ $bottomN
     * @since 4.7
     * @mongodb.server.release 5.2
     */
    public static <OutExpression, NExpression> WindowOutputField bottomN(
            final String path, final Bson sortBy, final OutExpression outExpression, final NExpression nExpression, @Nullable final Window window) {
        notNull("path", path);
        notNull("sortBy", sortBy);
        notNull("outExpression", outExpression);
        notNull("nExpression", nExpression);
        Map<ParamName, Object> args = new LinkedHashMap<>(3);
        args.put(ParamName.SORT_BY, sortBy);
        args.put(ParamName.OUTPUT, outExpression);
        args.put(ParamName.N_LOWERCASE, nExpression);
        return compoundParameterWindowFunction(path, "$bottomN", args, window);
    }

    /**
     * Builds a window output field of the evaluation result of the {@code expression} for the document whose position is shifted by the given
     * amount relative to the current document. If the shifted document is outside of the
     * {@linkplain Aggregates#setWindowFields(Object, Bson, Iterable) partition} containing the current document,
     * then the {@code defaultExpression} is used instead of the {@code expression}.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, Iterable) Sorting} is required.</p>
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
     * @return The constructed windowed output field.
     * @mongodb.driver.dochub core/window-functions-shift $shift
     */
    public static <TExpression> WindowOutputField shift(final String path, final TExpression expression,
                                                          @Nullable final TExpression defaultExpression, final int by) {
        notNull("path", path);
        notNull("expression", expression);
        Map<ParamName, Object> args = new LinkedHashMap<>(3);
        args.put(ParamName.OUTPUT, expression);
        args.put(ParamName.BY, by);
        if (defaultExpression != null) {
            args.put(ParamName.DEFAULT, defaultExpression);
        }
        return compoundParameterWindowFunction(path, "$shift", args, null);
    }

    /**
     * Builds a window output field of the order number of each document in its
     * {@linkplain Aggregates#setWindowFields(Object, Bson, Iterable) partition}.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, Iterable) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @return The constructed windowed output field.
     * @mongodb.driver.dochub core/window-functions-document-number $documentNumber
     */
    public static WindowOutputField documentNumber(final String path) {
        notNull("path", path);
        return simpleParameterWindowFunction(path, "$documentNumber", null, null);
    }

    /**
     * Builds a window output field of the rank of each document in its
     * {@linkplain Aggregates#setWindowFields(Object, Bson, Iterable) partition}.
     * Documents with the same value(s) of the {@linkplain Aggregates#setWindowFields(Object, Bson, Iterable) sortBy} fields result in
     * the same ranking and result in gaps in the returned ranks.
     * For example, a partition with the sequence [1, 3, 3, 5] representing the values of the single {@code sortBy} field
     * produces the following sequence of rank values: [1, 2, 2, 4].
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, Iterable) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @return The constructed windowed output field.
     * @mongodb.driver.dochub core/window-functions-rank $rank
     */
    public static WindowOutputField rank(final String path) {
        notNull("path", path);
        return simpleParameterWindowFunction(path, "$rank", null, null);
    }

    /**
     * Builds a window output field of the dense rank of each document in its
     * {@linkplain Aggregates#setWindowFields(Object, Bson, Iterable) partition}.
     * Documents with the same value(s) of the {@linkplain Aggregates#setWindowFields(Object, Bson, Iterable) sortBy} fields result in
     * the same ranking but do not result in gaps in the returned ranks.
     * For example, a partition with the sequence [1, 3, 3, 5] representing the values of the single {@code sortBy} field
     * produces the following sequence of rank values: [1, 2, 2, 3].
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, Iterable) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @return The constructed windowed output field.
     * @mongodb.driver.dochub core/window-functions-dense-rank $denseRank
     */
    public static WindowOutputField denseRank(final String path) {
        notNull("path", path);
        return simpleParameterWindowFunction(path, "$denseRank", null, null);
    }

    /**
     * Builds a window output field of the last observed non-{@link BsonType#NULL Null} evaluation result of the {@code expression}.
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param <TExpression> The expression type.
     * @return The constructed windowed output field.
     * @mongodb.driver.manual reference/operator/aggregation/locf/ $locf
     * @since 4.7
     * @mongodb.server.release 5.2
     */
    public static <TExpression> WindowOutputField locf(final String path, final TExpression expression) {
        notNull("path", path);
        notNull("expression", expression);
        return simpleParameterWindowFunction(path, "$locf", expression, null);
    }

    /**
     * Builds a window output field of a value that is equal to the evaluation result of the {@code expression} when it is non-{@link BsonType#NULL Null},
     * or to the linear interpolation of surrounding evaluation results of the {@code expression} when the result is {@link BsonType#NULL Null}.
     * <p>
     * {@linkplain Aggregates#setWindowFields(Object, Bson, Iterable) Sorting} is required.</p>
     *
     * @param path The output field path.
     * @param expression The expression.
     * @param <TExpression> The expression type.
     * @return The constructed windowed output field.
     * @mongodb.driver.manual reference/operator/aggregation/linearFill/ $linearFill
     * @since 4.7
     * @mongodb.server.release 5.3
     */
    public static <TExpression> WindowOutputField linearFill(final String path, final TExpression expression) {
        notNull("path", path);
        notNull("expression", expression);
        return simpleParameterWindowFunction(path, "$linearFill", expression, null);
    }

    private static WindowOutputField simpleParameterWindowFunction(final String path, final String functionName,
                                                                     @Nullable final Object expression,
                                                                     @Nullable final Window window) {
        return new BsonFieldWindowOutputField(new BsonField(path,
                new SimpleParameterFunctionAndWindow(functionName, expression, window)));
    }

    private static WindowOutputField compoundParameterWindowFunction(final String path, final String functionName,
                                                                       final Map<ParamName, Object> args,
                                                                       @Nullable final Window window) {
        return new BsonFieldWindowOutputField(new BsonField(path,
                new CompoundParameterFunctionAndWindow(functionName, args, window)));
    }

    private WindowOutputFields() {
        throw new UnsupportedOperationException();
    }

    private static final class BsonFieldWindowOutputField implements WindowOutputField {
        private final BsonField wrapped;

        BsonFieldWindowOutputField(final BsonField field) {
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
            BsonFieldWindowOutputField that = (BsonFieldWindowOutputField) o;
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
            SimpleParameterFunctionAndWindow that = (SimpleParameterFunctionAndWindow) o;
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
        private final Map<WindowOutputFields.ParamName, Object> args;

        CompoundParameterFunctionAndWindow(final String functionName, final Map<WindowOutputFields.ParamName, Object> args,
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
            CompoundParameterFunctionAndWindow that = (CompoundParameterFunctionAndWindow) o;
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
        N_UPPERCASE("N"),
        N_LOWERCASE("n"),
        P_LOWERCASE("p"),
        ALPHA("alpha"),
        OUTPUT("output"),
        BY("by"),
        DEFAULT("default"),
        SORT_BY("sortBy"),
        METHOD("method");

        private final String value;

        ParamName(final String value) {
            this.value = value;
        }

        String value() {
            return value;
        }
    }
}
