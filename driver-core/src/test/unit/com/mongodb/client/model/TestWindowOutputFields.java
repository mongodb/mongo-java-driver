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
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Windows.documents;
import static com.mongodb.client.model.Windows.range;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class TestWindowOutputFields {
    private static final String NO_EXPRESSION = "{}";
    private static final String PATH = "newField";
    private static final Bson SORT_BY = ascending("sortByField");
    private static final Map.Entry<Integer, BsonValue> INT_EXPR = new AbstractMap.SimpleImmutableEntry<>(1, new BsonInt32(1));
    private static final Map.Entry<String, BsonValue> STR_EXPR =
            new AbstractMap.SimpleImmutableEntry<>("$fieldToRead", new BsonString("$fieldToRead"));
    private static final Map.Entry<Document, BsonDocument> DOC_EXPR = new AbstractMap.SimpleImmutableEntry<>(
            new Document("gid", "$partitionId"),
            new BsonDocument("gid", new BsonString("$partitionId")));
    private static final Map.Entry<Document, BsonDocument> DOC_INT_EXPR = new AbstractMap.SimpleImmutableEntry<>(
            new Document("$cond", new Document("if",
                    new Document("$eq", asList("$gid", true)))
                    .append("then", 2).append("else", 2)),
            new BsonDocument("$cond", new BsonDocument("if",
                    new BsonDocument("$eq", new BsonArray(asList(new BsonString("$gid"), BsonBoolean.TRUE))))
                    .append("then", new BsonInt32(2)).append("else", new BsonInt32(2))));
    private static final Window POSITION_BASED_WINDOW = documents(1, 2);
    private static final Window RANGE_BASED_WINDOW = range(1, 2);

    @Test
    void of() {
        WindowOutputField expected = WindowOutputFields.sum(PATH, STR_EXPR.getKey(), POSITION_BASED_WINDOW);
        WindowOutputField actual = WindowOutputFields.of(new BsonField(PATH, new Document("$sum", STR_EXPR.getKey())
                .append("window", POSITION_BASED_WINDOW.toBsonDocument())));
        assertAll(
                () -> assertEquals(expected.toBsonField().getName(), actual.toBsonField().getName()),
                () -> assertEquals(expected.toBsonField().getValue().toBsonDocument(), actual.toBsonField().getValue().toBsonDocument()));
    }

    @Test
    void simpleWindowFunctions() {
        Map<Object, BsonValue> expressions = new HashMap<>();
        expressions.put(INT_EXPR.getKey(), INT_EXPR.getValue());
        expressions.put(STR_EXPR.getKey(), STR_EXPR.getValue());
        Collection<Window> windows = asList(null, POSITION_BASED_WINDOW, RANGE_BASED_WINDOW);
        assertAll(
                () -> assertSimpleParameterWindowFunction("$sum", WindowOutputFields::sum, expressions, windows, false),
                () -> assertSimpleParameterWindowFunction("$avg", WindowOutputFields::avg, expressions, windows, false),
                () -> assertSimpleParameterWindowFunction("$stdDevSamp", WindowOutputFields::stdDevSamp, expressions, windows, false),
                () -> assertSimpleParameterWindowFunction("$stdDevPop", WindowOutputFields::stdDevPop, expressions, windows, false),
                () -> assertSimpleParameterWindowFunction("$min", WindowOutputFields::min, expressions, windows, false),
                () -> assertSimpleParameterWindowFunction("$max", WindowOutputFields::max, expressions, windows, false),
                () -> assertNoParameterWindowFunction("$count", WindowOutputFields::count, windows, false),
                () -> assertSimpleParameterWindowFunction("$push", WindowOutputFields::push, expressions, windows, false),
                () -> assertSimpleParameterWindowFunction("$addToSet", WindowOutputFields::addToSet, expressions, windows, false),
                () -> assertSimpleParameterWindowFunction("$first", WindowOutputFields::first, expressions, windows, false),
                () -> assertSimpleParameterWindowFunction("$last", WindowOutputFields::last, expressions, windows, false),
                () -> assertNoParameterNoWindowFunction("$documentNumber", WindowOutputFields::documentNumber),
                () -> assertNoParameterNoWindowFunction("$rank", WindowOutputFields::rank),
                () -> assertNoParameterNoWindowFunction("$denseRank", WindowOutputFields::denseRank),
                () -> assertSimpleParameterNoWindowFunction("$locf", WindowOutputFields::locf, expressions),
                () -> assertSimpleParameterNoWindowFunction("$linearFill", WindowOutputFields::linearFill, expressions)
        );
    }

    @Test
    void derivative() {
        assertDerivativeOrIntegral("$derivative", WindowOutputFields::derivative);
    }

    @Test
    void timeDerivative() {
        assertTimeDerivativeOrIntegral("$derivative", WindowOutputFields::timeDerivative);
    }

    @Test
    void integral() {
        assertDerivativeOrIntegral("$integral", WindowOutputFields::integral);
    }

    @Test
    void timeIntegral() {
        assertTimeDerivativeOrIntegral("$integral", WindowOutputFields::timeIntegral);
    }

    @Test
    void covarianceSamp() {
        assertCovariance("$covarianceSamp", WindowOutputFields::covarianceSamp);
    }

    @Test
    void covariancePop() {
        assertCovariance("$covariancePop", WindowOutputFields::covariancePop);
    }

    @Test
    void expMovingAvgWithN() {
        assertWindowOutputField(
                new BsonField(PATH, new BsonDocument("$expMovingAvg", new BsonDocument("input", STR_EXPR.getValue())
                        .append("N", new BsonInt32(1)))),
                WindowOutputFields.expMovingAvg(PATH, STR_EXPR.getKey(), 1));
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                                WindowOutputFields.expMovingAvg(null, STR_EXPR.getKey(), 1)),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        WindowOutputFields.expMovingAvg(PATH, null, 1)),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        WindowOutputFields.expMovingAvg(PATH, STR_EXPR.getKey(), 0)),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        WindowOutputFields.expMovingAvg(PATH, STR_EXPR.getKey(), -1)));
    }

    @Test
    void expMovingAvgWithAlpha() {
        assertWindowOutputField(
                new BsonField(PATH, new BsonDocument("$expMovingAvg", new BsonDocument("input", STR_EXPR.getValue())
                        .append("alpha", new BsonDouble(0.5)))),
                WindowOutputFields.expMovingAvg(PATH, STR_EXPR.getKey(), 0.5));
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                        WindowOutputFields.expMovingAvg(null, STR_EXPR.getKey(), 0.5)),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        WindowOutputFields.expMovingAvg(PATH, null, 0.5)),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        WindowOutputFields.expMovingAvg(PATH, STR_EXPR.getKey(), 0d)),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        WindowOutputFields.expMovingAvg(PATH, STR_EXPR.getKey(), 1d)));
    }

    @Test
    void shift() {
        assertAll(
                () -> assertWindowOutputField(
                        new BsonField(PATH, new BsonDocument("$shift", new BsonDocument("output", STR_EXPR.getValue())
                                .append("by", new BsonInt32(-1))
                                .append("default", INT_EXPR.getValue()))),
                        WindowOutputFields.shift(PATH, STR_EXPR.getKey(), INT_EXPR.getKey(), -1)),
                () -> assertWindowOutputField(
                        new BsonField(PATH, new BsonDocument("$shift", new BsonDocument("output", STR_EXPR.getValue())
                                .append("by", new BsonInt32(0)))),
                        WindowOutputFields.shift(PATH, STR_EXPR.getKey(), null, 0)),
                () -> assertWindowOutputField(
                        new BsonField(PATH, new BsonDocument("$shift", new BsonDocument("output", STR_EXPR.getValue())
                                .append("by", new BsonInt32(1))
                                .append("default", INT_EXPR.getValue()))),
                        WindowOutputFields.shift(PATH, STR_EXPR.getKey(), INT_EXPR.getKey(), 1)));
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                        WindowOutputFields.shift(null, STR_EXPR.getKey(), INT_EXPR.getKey(), 0)),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        WindowOutputFields.shift(PATH, null, INT_EXPR.getKey(), 0)));
    }

    @Test
    void pick() {
        Map<Object, BsonValue> expressions = new HashMap<>();
        expressions.put(INT_EXPR.getKey(), INT_EXPR.getValue());
        expressions.put(STR_EXPR.getKey(), STR_EXPR.getValue());
        expressions.put(DOC_EXPR.getKey(), DOC_EXPR.getValue());
        Map<Object, BsonValue> nExpressions = new HashMap<>();
        nExpressions.put(INT_EXPR.getKey(), INT_EXPR.getValue());
        nExpressions.put(DOC_INT_EXPR.getKey(), DOC_INT_EXPR.getValue());
        Collection<Window> windows = asList(null, POSITION_BASED_WINDOW, RANGE_BASED_WINDOW);
        assertAll(
                () -> assertPickNoSortWindowFunction("$minN", WindowOutputFields::minN, expressions, "input", nExpressions, windows),
                () -> assertPickNoSortWindowFunction("$maxN", WindowOutputFields::maxN, expressions, "input", nExpressions, windows),
                () -> assertPickNoSortWindowFunction("$firstN", WindowOutputFields::firstN, expressions, "input", nExpressions, windows),
                () -> assertPickNoSortWindowFunction("$lastN", WindowOutputFields::lastN, expressions, "input", nExpressions, windows),
                () -> assertPickNoNWindowFunction("$bottom", WindowOutputFields::bottom, expressions, "output", windows),
                () -> assertPickSortWindowFunction("$bottomN", WindowOutputFields::bottomN, expressions, "output", nExpressions, windows),
                () -> assertPickNoNWindowFunction("$top", WindowOutputFields::top, expressions, "output", windows),
                () -> assertPickSortWindowFunction("$topN", WindowOutputFields::topN, expressions, "output", nExpressions, windows)
        );
    }

    private static void assertPickNoSortWindowFunction(
            final String expectedFunctionName,
            final QuadriFunction<String, Object, Object, Window, WindowOutputField> windowOutputFieldBuilder,
            final Map<Object, BsonValue> expressions,
            final String expressionKey,
            final Map<Object, BsonValue> nExpressions,
            final Collection<Window> windows) {
        assertPickWindowFunction(
                expectedFunctionName,
                (a1, ignoredSort, a3, a4, a5) -> windowOutputFieldBuilder.apply(a1, a3, a4, a5),
                false, expressions, expressionKey, nExpressions, windows);
    }

    private static void assertPickNoNWindowFunction(
            final String expectedFunctionName,
            final QuadriFunction<String, Bson, Object, Window, WindowOutputField> windowOutputFieldBuilder,
            final Map<Object, BsonValue> expressions,
            final String expressionKey,
            final Collection<Window> windows) {
        assertPickWindowFunction(
                expectedFunctionName,
                (a1, a2, a3, ignoredN, a5) -> windowOutputFieldBuilder.apply(a1, a2, a3, a5),
                true, expressions, expressionKey, Collections.singletonMap(NO_EXPRESSION, BsonDocument.parse(NO_EXPRESSION)), windows);
    }

    private static void assertPickSortWindowFunction(
            final String expectedFunctionName,
            final QuinqueFunction<String, Bson, Object, Object, Window, WindowOutputField> windowOutputFieldBuilder,
            final Map<Object, BsonValue> expressions,
            final String expressionKey,
            final Map<Object, BsonValue> nExpressions,
            final Collection<Window> windows) {
        assertPickWindowFunction(
                expectedFunctionName,
                windowOutputFieldBuilder,
                true, expressions, expressionKey, nExpressions, windows);
    }

    private static void assertPickWindowFunction(
            final String expectedFunctionName,
            final QuinqueFunction<String, Bson, Object, Object, Window, WindowOutputField> windowOutputFieldBuilder,
            final boolean useSortBy,
            final Map<Object, BsonValue> expressions,
            final String expressionKey,
            final Map<Object, BsonValue> nExpressions,
            final Collection<Window> windows) {
        Bson sortBySpec = useSortBy ? SORT_BY : null;
        for (final Map.Entry<Object, BsonValue> expressionAndEncoded: expressions.entrySet()) {
            Object expression = expressionAndEncoded.getKey();
            BsonValue encodedExpression = expressionAndEncoded.getValue();
            for (final Map.Entry<Object, BsonValue> nExpressionAndEncoded: nExpressions.entrySet()) {
                Object nExpression = nExpressionAndEncoded.getKey();
                BsonValue encodedNExpression = nExpressionAndEncoded.getValue();
                boolean useNExpression = !nExpression.equals(NO_EXPRESSION);
                for (final Window window : windows) {
                    BsonDocument expectedFunctionDoc = new BsonDocument(expressionKey, encodedExpression);
                    if (useSortBy) {
                        expectedFunctionDoc.append("sortBy", assertNotNull(sortBySpec).toBsonDocument());
                    }
                    if (useNExpression) {
                        expectedFunctionDoc.append("n", encodedNExpression);
                    }
                    BsonDocument expectedFunctionAndWindow = new BsonDocument(expectedFunctionName, expectedFunctionDoc);
                    if (window != null) {
                        expectedFunctionAndWindow.append("window", window.toBsonDocument());
                    }
                    BsonField expectedWindowOutputField = new BsonField(PATH, expectedFunctionAndWindow);
                    Supplier<String> msg = () -> "expectedFunctionName=" + expectedFunctionName
                            + ", path=" + PATH
                            + ", sortBySpec=" + sortBySpec
                            + ", expression=" + expression
                            + ", nExpression=" + nExpression
                            + ", window=" + window;
                    assertWindowOutputField(
                            expectedWindowOutputField, windowOutputFieldBuilder.apply(PATH, sortBySpec, expression, nExpression, window), msg);
                    assertThrows(IllegalArgumentException.class, () ->
                            windowOutputFieldBuilder.apply(null, sortBySpec, expression, nExpression, window), msg);
                    if (useSortBy) {
                        assertThrows(IllegalArgumentException.class, () ->
                                windowOutputFieldBuilder.apply(PATH, null, expression, nExpression, window), msg);
                    }
                    assertThrows(IllegalArgumentException.class, () ->
                            windowOutputFieldBuilder.apply(PATH, sortBySpec, null, nExpression, window), msg);
                    if (useNExpression) {
                        assertThrows(IllegalArgumentException.class, () ->
                                windowOutputFieldBuilder.apply(PATH, sortBySpec, expression, null, window), msg);
                    }
                }
            }
        }
    }

    private static void assertSimpleParameterWindowFunction(final String expectedFunctionName,
                                                            final TriFunction<String, Object, Window, WindowOutputField>
                                                                    windowOutputFieldBuilder,
                                                            final Map<Object, BsonValue> expressions,
                                                            final Collection<Window> windows,
                                                            final boolean windowRequired) {
        boolean assertNullExpressionsNotAllowed = !expressions.containsKey(NO_EXPRESSION);
        for (final Map.Entry<Object, BsonValue> expressionAndEncoded: expressions.entrySet()) {
            Object expression = expressionAndEncoded.getKey();
            BsonValue encodedExpression = expressionAndEncoded.getValue();
            for (final Window window : windows) {
                BsonDocument expectedFunctionAndWindow = new BsonDocument(expectedFunctionName, encodedExpression);
                if (window != null) {
                    expectedFunctionAndWindow.append("window", window.toBsonDocument());
                }
                BsonField expectedWindowOutputField = new BsonField(PATH, expectedFunctionAndWindow);
                Supplier<String> msg = () -> "expectedFunctionName=" + expectedFunctionName
                        + ", path=" + PATH
                        + ", expression=" + expression
                        + ", window=" + window
                        + ", windowRequired=" + windowRequired;
                if (windowRequired && window == null) {
                    assertThrows(IllegalArgumentException.class, () -> windowOutputFieldBuilder.apply(PATH, expression, null), msg);
                } else {
                    assertWindowOutputField(expectedWindowOutputField, windowOutputFieldBuilder.apply(PATH, expression, window), msg);
                }
                assertThrows(IllegalArgumentException.class, () -> windowOutputFieldBuilder.apply(null, expression, window), msg);
                if (assertNullExpressionsNotAllowed) {
                    assertThrows(IllegalArgumentException.class, () -> windowOutputFieldBuilder.apply(PATH, null, window), msg);
                }
            }
        }
    }

    private static void assertNoParameterWindowFunction(final String expectedFunctionName,
                                                        final BiFunction<String, Window, WindowOutputField> windowOutputFieldBuilder,
                                                        final Collection<Window> windows, final boolean windowRequired) {
        assertSimpleParameterWindowFunction(expectedFunctionName,
                (fName, expr, window) -> windowOutputFieldBuilder.apply(fName, window),
                Collections.singletonMap(NO_EXPRESSION, BsonDocument.parse(NO_EXPRESSION)), windows, windowRequired);
    }

    private static void assertSimpleParameterNoWindowFunction(
            final String expectedFunctionName,
            final BiFunction<String, Object, WindowOutputField> windowOutputFieldBuilder,
            final Map<Object, BsonValue> expressions) {
        assertSimpleParameterWindowFunction(
                expectedFunctionName,
                (fName, expr, window) -> windowOutputFieldBuilder.apply(fName, expr),
                expressions, singleton(null), false);
    }

    private static void assertNoParameterNoWindowFunction(final String expectedFunctionName,
                                                          final Function<String, WindowOutputField> windowOutputFieldBuilder) {
        assertNoParameterWindowFunction(expectedFunctionName, (fName, window) -> windowOutputFieldBuilder.apply(fName),
                singleton(null), false);
    }

    private static void assertWindowOutputField(final BsonField expected, final WindowOutputField actual,
                                                  @Nullable final Supplier<String> messageSupplier) {
        assertEquals(expected.getName(), actual.toBsonField().getName(), messageSupplier);
        assertEquals(expected.getValue().toBsonDocument(), actual.toBsonField().getValue().toBsonDocument(), messageSupplier);
    }

    private static void assertWindowOutputField(final BsonField expected, final WindowOutputField actual) {
        assertWindowOutputField(expected, actual, null);
    }

    private static void assertDerivativeOrIntegral(final String expectedFunctionName,
                                                   final TriFunction<String, Object, Window, WindowOutputField>
                                                           windowOutputFieldBuilder) {
        assertDerivativeOrIntegral(expectedFunctionName,
                (fName, expr, window, unit) -> windowOutputFieldBuilder.apply(fName, expr, window), false);
    }

    private static void assertTimeDerivativeOrIntegral(final String expectedFunctionName,
                                                       final QuadriFunction<String, Object, Window, MongoTimeUnit, WindowOutputField>
                                                               windowOutputFieldBuilder) {
        assertDerivativeOrIntegral(expectedFunctionName, windowOutputFieldBuilder, true);
    }

    private static void assertDerivativeOrIntegral(final String expectedFunctionName,
                                                   final QuadriFunction<String, Object, Window, MongoTimeUnit, WindowOutputField>
                                                           windowOutputFieldBuilder,
                                                   final boolean time) {
        BsonDocument expectedArgs = new BsonDocument("input", STR_EXPR.getValue());
        if (time) {
            expectedArgs.append("unit", new BsonString(MongoTimeUnit.DAY.value()));
        }
        assertWindowOutputField(new BsonField(PATH,
                        new BsonDocument(expectedFunctionName, expectedArgs)
                                .append("window", POSITION_BASED_WINDOW.toBsonDocument())),
                windowOutputFieldBuilder.apply(PATH, STR_EXPR.getKey(), POSITION_BASED_WINDOW, MongoTimeUnit.DAY));
        assertThrows(IllegalArgumentException.class, () ->
                        windowOutputFieldBuilder.apply(PATH, STR_EXPR.getKey(), null, MongoTimeUnit.DAY));
        if (time) {
            assertThrows(IllegalArgumentException.class, () ->
                    windowOutputFieldBuilder.apply(PATH, STR_EXPR.getKey(), POSITION_BASED_WINDOW, null));
        }
    }

    private static void assertCovariance(final String expectedFunctionName,
                                         final QuadriFunction<String, Object, Object, Window, WindowOutputField>
                                                 windowOutputFieldBuilder) {
        assertWindowOutputField(new BsonField(PATH,
                        new BsonDocument(expectedFunctionName, new BsonArray(asList(INT_EXPR.getValue(), STR_EXPR.getValue())))
                                .append("window", POSITION_BASED_WINDOW.toBsonDocument())),
                windowOutputFieldBuilder.apply(PATH, INT_EXPR.getKey(), STR_EXPR.getKey(), POSITION_BASED_WINDOW));
        assertWindowOutputField(new BsonField(PATH,
                        new BsonDocument(expectedFunctionName, new BsonArray(asList(INT_EXPR.getValue(), STR_EXPR.getValue())))),
                windowOutputFieldBuilder.apply(PATH, INT_EXPR.getKey(), STR_EXPR.getKey(), null));
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                        windowOutputFieldBuilder.apply(PATH, null, STR_EXPR.getKey(), POSITION_BASED_WINDOW)),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        windowOutputFieldBuilder.apply(PATH, INT_EXPR.getKey(), null, POSITION_BASED_WINDOW)));
    }

    @FunctionalInterface
    interface TriFunction<A1, A2, A3, R> {
        R apply(@Nullable A1 a1, @Nullable A2 a2, @Nullable A3 a3);
    }

    @FunctionalInterface
    interface QuadriFunction<A1, A2, A3, A4, R> {
        R apply(@Nullable A1 a1, @Nullable A2 a2, @Nullable A3 a3, @Nullable A4 a4);
    }

    @FunctionalInterface
    interface QuinqueFunction<A1, A2, A3, A4, A5, R> {
        R apply(@Nullable A1 a1, @Nullable A2 a2, @Nullable A3 a3, @Nullable A4 a4, @Nullable A5 a5);
    }
}
