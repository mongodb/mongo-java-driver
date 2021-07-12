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

import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.mongodb.client.model.Windows.documents;
import static com.mongodb.client.model.Windows.range;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class TestWindowedComputations {
    private static final String NO_EXPRESSION = "{}";
    private static final String PATH = "newField";
    private static final Map.Entry<Integer, BsonValue> INT_EXPR = new AbstractMap.SimpleImmutableEntry<>(1, new BsonInt32(1));
    private static final Map.Entry<String, BsonValue> STR_EXPR =
            new AbstractMap.SimpleImmutableEntry<>("$fieldToRead", new BsonString("$fieldToRead"));
    private static final Window POSITION_BASED_WINDOW = documents(1, 2);
    private static final Window RANGE_BASED_WINDOW = range(1, 2);

    @Test
    void of() {
        WindowedComputation expected = WindowedComputations.sum(PATH, STR_EXPR.getKey(), POSITION_BASED_WINDOW);
        WindowedComputation actual = WindowedComputations.of(new BsonField(PATH, new Document("$sum", STR_EXPR.getKey())
                .append("window", POSITION_BASED_WINDOW.toBsonDocument())));
        assertAll(
                () -> assertEquals(expected.asBsonField().getName(), actual.asBsonField().getName()),
                () -> assertEquals(expected.asBsonField().getValue().toBsonDocument(), actual.asBsonField().getValue().toBsonDocument()));
    }

    @Test
    void simpleWindowFunctions() {
        final Map<Object, BsonValue> expressions = new HashMap<>();
        expressions.put(INT_EXPR.getKey(), INT_EXPR.getValue());
        expressions.put(STR_EXPR.getKey(), STR_EXPR.getValue());
        final Collection<Window> windows = asList(null, POSITION_BASED_WINDOW, RANGE_BASED_WINDOW);
        assertAll(
                () -> assertSimpleParameterWindowFunction("$sum", WindowedComputations::sum, expressions, windows, false),
                () -> assertSimpleParameterWindowFunction("$avg", WindowedComputations::avg, expressions, windows, false),
                () -> assertSimpleParameterWindowFunction("$stdDevSamp", WindowedComputations::stdDevSamp, expressions, windows, false),
                () -> assertSimpleParameterWindowFunction("$stdDevPop", WindowedComputations::stdDevPop, expressions, windows, false),
                () -> assertSimpleParameterWindowFunction("$min", WindowedComputations::min, expressions, windows, false),
                () -> assertSimpleParameterWindowFunction("$max", WindowedComputations::max, expressions, windows, false),
                () -> assertNoParameterWindowFunction("$count", WindowedComputations::count, windows, false),
                () -> assertSimpleParameterWindowFunction("$push", WindowedComputations::push, expressions, windows, false),
                () -> assertSimpleParameterWindowFunction("$addToSet", WindowedComputations::addToSet, expressions, windows, false),
                () -> assertSimpleParameterWindowFunction("$first", WindowedComputations::first, expressions, windows, false),
                () -> assertSimpleParameterWindowFunction("$last", WindowedComputations::last, expressions, windows, false),
                () -> assertNoParameterNoWindowFunction("$documentNumber", WindowedComputations::documentNumber),
                () -> assertNoParameterNoWindowFunction("$rank", WindowedComputations::rank),
                () -> assertNoParameterNoWindowFunction("$denseRank", WindowedComputations::denseRank)
        );
    }

    @Test
    void derivative() {
        assertDerivativeOrIntegral("$derivative", WindowedComputations::derivative);
    }

    @Test
    void timeDerivative() {
        assertTimeDerivativeOrIntegral("$derivative", WindowedComputations::timeDerivative);
    }

    @Test
    void integral() {
        assertDerivativeOrIntegral("$integral", WindowedComputations::integral);
    }

    @Test
    void timeIntegral() {
        assertTimeDerivativeOrIntegral("$integral", WindowedComputations::timeIntegral);
    }

    @Test
    void covarianceSamp() {
        assertCovariance("$covarianceSamp", WindowedComputations::covarianceSamp);
    }

    @Test
    void covariancePop() {
        assertCovariance("$covariancePop", WindowedComputations::covariancePop);
    }

    @Test
    void expMovingAvgWithN() {
        assertWindowedComputation(
                new BsonField(PATH, new BsonDocument("$expMovingAvg", new BsonDocument("input", STR_EXPR.getValue())
                        .append("N", new BsonInt32(1)))),
                WindowedComputations.expMovingAvg(PATH, STR_EXPR.getKey(), 1));
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                                WindowedComputations.expMovingAvg(null, STR_EXPR.getKey(), 1)),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        WindowedComputations.expMovingAvg(PATH, null, 1)),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        WindowedComputations.expMovingAvg(PATH, STR_EXPR.getKey(), 0)),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        WindowedComputations.expMovingAvg(PATH, STR_EXPR.getKey(), -1)));
    }

    @Test
    void expMovingAvgWithAlpha() {
        assertWindowedComputation(
                new BsonField(PATH, new BsonDocument("$expMovingAvg", new BsonDocument("input", STR_EXPR.getValue())
                        .append("alpha", new BsonDouble(0.5)))),
                WindowedComputations.expMovingAvg(PATH, STR_EXPR.getKey(), 0.5));
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                        WindowedComputations.expMovingAvg(null, STR_EXPR.getKey(), 0.5)),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        WindowedComputations.expMovingAvg(PATH, null, 0.5)),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        WindowedComputations.expMovingAvg(PATH, STR_EXPR.getKey(), 0d)),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        WindowedComputations.expMovingAvg(PATH, STR_EXPR.getKey(), 1d)));
    }

    @Test
    void shift() {
        assertAll(
                () -> assertWindowedComputation(
                        new BsonField(PATH, new BsonDocument("$shift", new BsonDocument("output", STR_EXPR.getValue())
                                .append("by", new BsonInt32(-1))
                                .append("default", INT_EXPR.getValue()))),
                        WindowedComputations.shift(PATH, STR_EXPR.getKey(), INT_EXPR.getKey(), -1)),
                () -> assertWindowedComputation(
                        new BsonField(PATH, new BsonDocument("$shift", new BsonDocument("output", STR_EXPR.getValue())
                                .append("by", new BsonInt32(0)))),
                        WindowedComputations.shift(PATH, STR_EXPR.getKey(), null, 0)),
                () -> assertWindowedComputation(
                        new BsonField(PATH, new BsonDocument("$shift", new BsonDocument("output", STR_EXPR.getValue())
                                .append("by", new BsonInt32(1))
                                .append("default", INT_EXPR.getValue()))),
                        WindowedComputations.shift(PATH, STR_EXPR.getKey(), INT_EXPR.getKey(), 1)));
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                        WindowedComputations.shift(null, STR_EXPR.getKey(), INT_EXPR.getKey(), 0)),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        WindowedComputations.shift(PATH, null, INT_EXPR.getKey(), 0)));
    }

    private static void assertSimpleParameterWindowFunction(final String expectedFunctionName,
                                                            final TriFunction<String, Object, Window, WindowedComputation>
                                                                    windowedComputationBuilder,
                                                            final Map<Object, BsonValue> expressions,
                                                            final Collection<Window> windows,
                                                            final boolean windowRequired) {
        boolean assertNullExpressionsNotAllowed = !expressions.containsKey(NO_EXPRESSION);
        for (final Map.Entry<Object, BsonValue> expressionAndEncoded: expressions.entrySet()) {
            final Object expression = expressionAndEncoded.getKey();
            final BsonValue encodedExpression = expressionAndEncoded.getValue();
            for (final Window window : windows) {
                final BsonDocument expectedFunctionAndWindow = new BsonDocument(expectedFunctionName, encodedExpression);
                if (window != null) {
                    expectedFunctionAndWindow.append("window", window.toBsonDocument());
                }
                BsonField expectedWindowedComputation = new BsonField(PATH, expectedFunctionAndWindow);
                Supplier<String> msg = () -> "expectedFunctionName=" + expectedFunctionName
                        + "path=" + PATH
                        + "expression=" + expression
                        + "window=" + window
                        + "windowRequired=" + windowRequired;
                if (windowRequired && window == null) {
                    assertThrows(IllegalArgumentException.class, () -> windowedComputationBuilder.apply(PATH, expression, null), msg);
                } else {
                    assertWindowedComputation(expectedWindowedComputation, windowedComputationBuilder.apply(PATH, expression, window), msg);
                }
                assertThrows(IllegalArgumentException.class, () -> windowedComputationBuilder.apply(null, expression, window), msg);
                if (assertNullExpressionsNotAllowed) {
                    assertThrows(IllegalArgumentException.class, () -> windowedComputationBuilder.apply(PATH, null, window), msg);
                }
            }
        }
    }

    private static void assertNoParameterWindowFunction(final String expectedFunctionName,
                                                        final BiFunction<String, Window, WindowedComputation> windowedComputationBuilder,
                                                        final Collection<Window> windows, final boolean windowRequired) {
        assertSimpleParameterWindowFunction(expectedFunctionName,
                (fName, expr, window) -> windowedComputationBuilder.apply(fName, window),
                Collections.singletonMap(NO_EXPRESSION, BsonDocument.parse(NO_EXPRESSION)), windows, windowRequired);
    }

    private static void assertNoParameterNoWindowFunction(final String expectedFunctionName,
                                                          final Function<String, WindowedComputation> windowedComputationBuilder) {
        assertNoParameterWindowFunction(expectedFunctionName, (fName, window) -> windowedComputationBuilder.apply(fName),
                Collections.singleton(null), false);
    }

    private static void assertWindowedComputation(final BsonField expected, final WindowedComputation actual,
                                                  @Nullable final Supplier<String> messageSupplier) {
        assertEquals(expected.getName(), actual.asBsonField().getName(), messageSupplier);
        assertEquals(expected.getValue().toBsonDocument(), actual.asBsonField().getValue().toBsonDocument(), messageSupplier);
    }

    private static void assertWindowedComputation(final BsonField expected, final WindowedComputation actual) {
        assertWindowedComputation(expected, actual, null);
    }

    private static void assertDerivativeOrIntegral(final String expectedFunctionName,
                                                   final TriFunction<String, Object, Window, WindowedComputation>
                                                           windowedComputationBuilder) {
        assertDerivativeOrIntegral(expectedFunctionName,
                (fName, expr, window, unit) -> windowedComputationBuilder.apply(fName, expr, window), false);
    }

    private static void assertTimeDerivativeOrIntegral(final String expectedFunctionName,
                                                       final QuadriFunction<String, Object, Window, MongoTimeUnit, WindowedComputation>
                                                               windowedComputationBuilder) {
        assertDerivativeOrIntegral(expectedFunctionName, windowedComputationBuilder, true);
    }

    private static void assertDerivativeOrIntegral(final String expectedFunctionName,
                                                   final QuadriFunction<String, Object, Window, MongoTimeUnit, WindowedComputation>
                                                           windowedComputationBuilder,
                                                   final boolean time) {
        final BsonDocument expectedArgs = new BsonDocument("input", STR_EXPR.getValue());
        if (time) {
            expectedArgs.append("unit", new BsonString(MongoTimeUnit.DAY.value()));
        }
        assertWindowedComputation(new BsonField(PATH,
                        new BsonDocument(expectedFunctionName, expectedArgs)
                                .append("window", POSITION_BASED_WINDOW.toBsonDocument())),
                windowedComputationBuilder.apply(PATH, STR_EXPR.getKey(), POSITION_BASED_WINDOW, MongoTimeUnit.DAY));
        assertThrows(IllegalArgumentException.class, () ->
                        windowedComputationBuilder.apply(PATH, STR_EXPR.getKey(), null, MongoTimeUnit.DAY));
        if (time) {
            assertThrows(IllegalArgumentException.class, () ->
                    windowedComputationBuilder.apply(PATH, STR_EXPR.getKey(), POSITION_BASED_WINDOW, null));
        }
    }

    private static void assertCovariance(final String expectedFunctionName,
                                         final QuadriFunction<String, Object, Object, Window, WindowedComputation>
                                                 windowedComputationBuilder) {
        assertWindowedComputation(new BsonField(PATH,
                        new BsonDocument(expectedFunctionName, new BsonArray(asList(INT_EXPR.getValue(), STR_EXPR.getValue())))
                                .append("window", POSITION_BASED_WINDOW.toBsonDocument())),
                windowedComputationBuilder.apply(PATH, INT_EXPR.getKey(), STR_EXPR.getKey(), POSITION_BASED_WINDOW));
        assertWindowedComputation(new BsonField(PATH,
                        new BsonDocument(expectedFunctionName, new BsonArray(asList(INT_EXPR.getValue(), STR_EXPR.getValue())))),
                windowedComputationBuilder.apply(PATH, INT_EXPR.getKey(), STR_EXPR.getKey(), null));
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                        windowedComputationBuilder.apply(PATH, null, STR_EXPR.getKey(), POSITION_BASED_WINDOW)),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        windowedComputationBuilder.apply(PATH, INT_EXPR.getKey(), null, POSITION_BASED_WINDOW)));
    }

    @FunctionalInterface
    interface TriFunction<A1, A2, A3, R> {
        R apply(@Nullable A1 a1, @Nullable A2 a2, @Nullable A3 a3);
    }

    @FunctionalInterface
    interface QuadriFunction<A1, A2, A3, A4, R> {
        R apply(@Nullable A1 a1, @Nullable A2 a2, @Nullable A3 a3, @Nullable A4 a4);
    }
}
