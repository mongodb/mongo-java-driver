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

import com.mongodb.assertions.Assertions;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.expressions.MqlExpression.AstPlaceholder;
import static com.mongodb.client.model.expressions.MqlExpression.extractBsonValue;

/**
 * Convenience methods related to {@link Expression}, used primarily to
 * produce values in the context of the MongoDB Query Language (MQL).
 */
public final class Expressions {

    private Expressions() {}

    /**
     * Returns a {@linkplain BooleanExpression boolean} value corresponding to
     * the provided {@code boolean} primitive.
     *
     * @param of the {@code boolean} primitive.
     * @return the resulting value.
     */
    public static BooleanExpression of(final boolean of) {
        // we intentionally disallow ofBoolean(null)
        return new MqlExpression<>((codecRegistry) -> new AstPlaceholder(new BsonBoolean(of)));
    }

    public static ArrayExpression<BooleanExpression> ofBooleanArray(final boolean... array) {
        Assertions.notNull("array", array);
        List<BsonValue> list = new ArrayList<>();
        for (boolean b : array) {
            list.add(new BsonBoolean(b));
        }
        return new MqlExpression<>((cr) -> new AstPlaceholder(new BsonArray(list)));
    }

    /**
     * Returns an {@linkplain IntegerExpression integer} value corresponding to
     * the provided {@code int} primitive.
     *
     * @param of the {@code int} primitive.
     * @return the resulting value.
     */
    public static IntegerExpression of(final int of) {
        return new MqlExpression<>((codecRegistry) -> new AstPlaceholder(new BsonInt32(of)));
    }

    public static ArrayExpression<IntegerExpression> ofIntegerArray(final int... array) {
        Assertions.notNull("array", array);
        List<BsonValue> list = new ArrayList<>();
        for (int i : array) {
            list.add(new BsonInt32(i));
        }
        return new MqlExpression<>((cr) -> new AstPlaceholder(new BsonArray(list)));
    }

    /**
     * Returns an {@linkplain IntegerExpression integer} value corresponding to
     * the provided {@code long} primitive.
     *
     * @param of the {@code long} primitive.
     * @return the resulting value.
     */
    public static IntegerExpression of(final long of) {
        return new MqlExpression<>((codecRegistry) -> new AstPlaceholder(new BsonInt64(of)));
    }

    public static ArrayExpression<IntegerExpression> ofIntegerArray(final long... array) {
        Assertions.notNull("array", array);
        List<BsonValue> list = new ArrayList<>();
        for (long i : array) {
            list.add(new BsonInt64(i));
        }
        return new MqlExpression<>((cr) -> new AstPlaceholder(new BsonArray(list)));
    }

    /**
     * Returns a {@linkplain NumberExpression number} value corresponding to
     * the provided {@code double} primitive.
     *
     * @param of the {@code double} primitive.
     * @return the resulting value.
     */
    public static NumberExpression of(final double of) {
        return new MqlExpression<>((codecRegistry) -> new AstPlaceholder(new BsonDouble(of)));
    }

    public static ArrayExpression<NumberExpression> ofNumberArray(final double... array) {
        Assertions.notNull("array", array);
        List<BsonValue> list = new ArrayList<>();
        for (double n : array) {
            list.add(new BsonDouble(n));
        }
        return new MqlExpression<>((cr) -> new AstPlaceholder(new BsonArray(list)));
    }

    /**
     * Returns a {@linkplain NumberExpression number} value corresponding to
     * the provided {@link Decimal128}
     *
     * @param of the {@link Decimal128}.
     * @return the resulting value.
     */
    public static NumberExpression of(final Decimal128 of) {
        Assertions.notNull("Decimal128", of);
        return new MqlExpression<>((codecRegistry) -> new AstPlaceholder(new BsonDecimal128(of)));
    }

    public static ArrayExpression<NumberExpression> ofNumberArray(final Decimal128... array) {
        Assertions.notNull("array", array);
        List<BsonValue> result = new ArrayList<>();
        for (Decimal128 e : array) {
            Assertions.notNull("elements of array", e);
            result.add(new BsonDecimal128(e));
        }
        return new MqlExpression<>((cr) -> new AstPlaceholder(new BsonArray(result)));
    }


    /**
     * Returns a {@linkplain DateExpression date and time} value corresponding to
     * the provided {@link Instant}.
     *
     * @param of the {@link Instant}.
     * @return the resulting value.
     */
    public static DateExpression of(final Instant of) {
        Assertions.notNull("Instant", of);
        return new MqlExpression<>((codecRegistry) -> new AstPlaceholder(new BsonDateTime(of.toEpochMilli())));
    }

    public static ArrayExpression<DateExpression> ofDateArray(final Instant... array) {
        Assertions.notNull("array", array);
        List<BsonValue> result = new ArrayList<>();
        for (Instant e : array) {
            Assertions.notNull("elements of array", e);
            result.add(new BsonDateTime(e.toEpochMilli()));
        }
        return new MqlExpression<>((cr) -> new AstPlaceholder(new BsonArray(result)));
    }

    /**
     * Returns an {@linkplain StringExpression string} value corresponding to
     * the provided {@link String}.
     *
     * @param of the {@link String}.
     * @return the resulting value.
     */
    public static StringExpression of(final String of) {
        Assertions.notNull("String", of);
        return new MqlExpression<>((codecRegistry) -> new AstPlaceholder(new BsonString(of)));
    }

    public static ArrayExpression<StringExpression> ofStringArray(final String... array) {
        Assertions.notNull("array", array);
        List<BsonValue> result = new ArrayList<>();
        for (String e : array) {
            Assertions.notNull("elements of array", e);
            result.add(new BsonString(e));
        }
        return new MqlExpression<>((cr) -> new AstPlaceholder(new BsonArray(result)));
    }

    /**
     * Returns the "current" {@linkplain DocumentExpression document} value.
     * The "current" value is the top-level document currently being processed
     * in the aggregation pipeline stage.
     *
     * @return the resulting value
     */
    public static DocumentExpression current() {
        return new MqlExpression<>((cr) -> new AstPlaceholder(new BsonString("$$CURRENT")))
                .assertImplementsAllExpressions();
    }

    public static <R extends Expression> MapExpression<R> currentAsMap() {
        return new MqlExpression<>((cr) -> new AstPlaceholder(new BsonString("$$CURRENT")))
                .assertImplementsAllExpressions();
    }

    /**
     * Returns the "current" value as a {@linkplain MapExpression map} value.
     * The "current" value is the top-level document currently being processed
     * in the aggregation pipeline stage.
     *
     * @return the resulting value
     * @param <R> the type of the resulting value
     */
    public static <R extends DocumentExpression> MapExpression<R> currentAsMap() {
        return new MqlExpression<>((cr) -> new AstPlaceholder(new BsonString("$$CURRENT")))
                .assertImplementsAllExpressions();
    }

    /**
     * Returns an {@linkplain DocumentExpression array} value, containing the
     * {@linkplain Expression values} provided.
     *
     * @param array the {@linkplain Expression values}.
     * @return the resulting value.
     * @param <T> the type of the array elements.
     */
    @SafeVarargs  // nothing is stored in the array
    public static <T extends Expression> ArrayExpression<T> ofArray(final T... array) {
        Assertions.notNull("array", array);
        return new MqlExpression<>((cr) -> {
            List<BsonValue> list = new ArrayList<>();
            for (T v : array) {
                Assertions.notNull("elements of array", v);
                list.add(((MqlExpression<?>) v).toBsonValue(cr));
            }
            return new AstPlaceholder(new BsonArray(list));
        });
    }

    /**
     * Returns an {@linkplain EntryExpression entry} value. An entry is a
     * {@linkplain StringExpression string} key and some
     * {@linkplain Expression value}. Entries are used with
     * {@linkplain MapExpression maps}. An entry
     * value may be equal to a document value with a field "k" equal to the
     * entry's key and a field "v" equal to the entry's value.
     *
     * @param k the key.
     * @param v the value.
     * @return the resulting value.
     * @param <T> the type of the key.
     */
    public static <T extends Expression> EntryExpression<T> ofEntry(final StringExpression k, final T v) {
        Assertions.notNull("k", k);
        Assertions.notNull("v", v);
        return new MqlExpression<>((cr) -> {
            BsonDocument document = new BsonDocument();
            document.put("k", extractBsonValue(cr, k));
            document.put("v", extractBsonValue(cr, v));
            return new AstPlaceholder(document);
        });
    }

    public static <T extends Expression> MapExpression<T> ofMap() {
        return ofMap(new BsonDocument());
    }

    /**
     * Returns a {@linkplain MapExpression map} value corresponding to the
     * provided {@link Bson Bson document}. The user asserts that all values
     * in the document are of type {@code T}.
     *
     * @param map the map as a {@link Bson Bson document}.
     * @return the resulting map value.
     * @param <T> the type of the resulting map's values.
     */
    public static <T extends Expression> MapExpression<T> ofMap(final Bson map) {
        Assertions.notNull("map", map);
        return new MqlExpression<>((cr) -> new AstPlaceholder(new BsonDocument("$literal",
                map.toBsonDocument(BsonDocument.class, cr))));
    }

    /**
     * Returns a {@linkplain DocumentExpression document} value corresponding to the
     * provided {@link Bson Bson document}.
     *
     * @param document the {@link Bson Bson document}.
     * @return the resulting value.
     */
    public static DocumentExpression of(final Bson document) {
        Assertions.notNull("document", document);
        // All documents are wrapped in a $literal. If we don't wrap, we need to
        // check for empty documents and documents that are actually expressions
        // (and need to be wrapped in $literal anyway). This would be brittle.
        return new MqlExpression<>((cr) -> new AstPlaceholder(new BsonDocument("$literal",
                document.toBsonDocument(BsonDocument.class, cr))));
    }

    /**
     * The null value in the context of the MongoDB Query Language (MQL).
     *
     * <p>The null value is not part of, and cannot be used as if it were part of,
     * any other type. It has no explicit type of its own. Instead of checking for
     * null, users should check for their expected type, via methods such as
     * {@link Expression#isNumberOr(NumberExpression)}. Where the null value must
     * be checked explicitly, users may use {@link Branches#isNull} within
     * {@link Expression#switchOn}.
     *
     * @return the null value
     */
    public static Expression ofNull() {
        // There is no specific expression type corresponding to Null,
        // and Null is not a value in any other expression type.
        return new MqlExpression<>((cr) -> new AstPlaceholder(new BsonNull()))
                .assertImplementsAllExpressions();
    }

    static NumberExpression numberToExpression(final Number number) {
        Assertions.notNull("number", number);
        if (number instanceof Integer) {
            return of((int) number);
        } else if (number instanceof Long) {
            return of((long) number);
        } else if (number instanceof Double) {
            return of((double) number);
        } else if (number instanceof Decimal128) {
            return of((Decimal128) number);
        } else {
            throw new IllegalArgumentException("Number must be one of: Integer, Long, Double, Decimal128");
        }
    }
}
