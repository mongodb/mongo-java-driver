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
import com.mongodb.annotations.Reason;
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

import static com.mongodb.client.model.mql.MqlExpression.AstPlaceholder;
import static com.mongodb.client.model.mql.MqlExpression.toBsonValue;
import static com.mongodb.client.model.mql.MqlUnchecked.Unchecked.TYPE_ARGUMENT;

/**
 * Convenience methods related to {@link MqlValue}, used primarily to
 * produce values in the context of the MongoDB Query Language (MQL).
 *
 * @since 4.9.0
 */
@Beta(Reason.CLIENT)
public final class MqlValues {

    private MqlValues() {}

    /**
     * Returns a {@linkplain MqlBoolean boolean} value corresponding to
     * the provided {@code boolean} primitive.
     *
     * @param of the {@code boolean} primitive.
     * @return the resulting value.
     */
    public static MqlBoolean of(final boolean of) {
        // we intentionally disallow ofBoolean(null)
        return new MqlExpression<>((codecRegistry) -> new AstPlaceholder(new BsonBoolean(of)));
    }

    /**
     * Returns an {@linkplain MqlArray array} of
     * {@linkplain MqlBoolean booleans} corresponding to
     * the provided {@code boolean} primitives.
     *
     * @param array the array.
     * @return the resulting value.
     */
    public static MqlArray<MqlBoolean> ofBooleanArray(final boolean... array) {
        Assertions.notNull("array", array);
        BsonArray bsonArray = new BsonArray();
        for (boolean b : array) {
            bsonArray.add(new BsonBoolean(b));
        }
        return new MqlExpression<>((cr) -> new AstPlaceholder(bsonArray));
    }

    /**
     * Returns an {@linkplain MqlInteger integer} value corresponding to
     * the provided {@code int} primitive.
     *
     * @param of the {@code int} primitive.
     * @return the resulting value.
     */
    public static MqlInteger of(final int of) {
        return new MqlExpression<>((codecRegistry) -> new AstPlaceholder(new BsonInt32(of)));
    }

    /**
     * Returns an {@linkplain MqlArray array} of
     * {@linkplain MqlInteger integers} corresponding to
     * the provided {@code int} primitives.
     *
     * @param array the array.
     * @return the resulting value.
     */
    public static MqlArray<MqlInteger> ofIntegerArray(final int... array) {
        Assertions.notNull("array", array);
        BsonArray bsonArray = new BsonArray();
        for (int i : array) {
            bsonArray.add(new BsonInt32(i));
        }
        return new MqlExpression<>((cr) -> new AstPlaceholder(bsonArray));
    }

    /**
     * Returns an {@linkplain MqlInteger integer} value corresponding to
     * the provided {@code long} primitive.
     *
     * @param of the {@code long} primitive.
     * @return the resulting value.
     */
    public static MqlInteger of(final long of) {
        return new MqlExpression<>((codecRegistry) -> new AstPlaceholder(new BsonInt64(of)));
    }

    /**
     * Returns an {@linkplain MqlArray array} of
     * {@linkplain MqlInteger integers} corresponding to
     * the provided {@code long} primitives.
     *
     * @param array the array.
     * @return the resulting value.
     */
    public static MqlArray<MqlInteger> ofIntegerArray(final long... array) {
        Assertions.notNull("array", array);
        BsonArray bsonArray = new BsonArray();
        for (long i : array) {
            bsonArray.add(new BsonInt64(i));
        }
        return new MqlExpression<>((cr) -> new AstPlaceholder(bsonArray));
    }

    /**
     * Returns a {@linkplain MqlNumber number} value corresponding to
     * the provided {@code double} primitive.
     *
     * @param of the {@code double} primitive.
     * @return the resulting value.
     */
    public static MqlNumber of(final double of) {
        return new MqlExpression<>((codecRegistry) -> new AstPlaceholder(new BsonDouble(of)));
    }

    /**
     * Returns an {@linkplain MqlArray array} of
     * {@linkplain MqlNumber numbers} corresponding to
     * the provided {@code double} primitives.
     *
     * @param array the array.
     * @return the resulting value.
     */
    public static MqlArray<MqlNumber> ofNumberArray(final double... array) {
        Assertions.notNull("array", array);
        BsonArray bsonArray = new BsonArray();
        for (double n : array) {
            bsonArray.add(new BsonDouble(n));
        }
        return new MqlExpression<>((cr) -> new AstPlaceholder(bsonArray));
    }

    /**
     * Returns a {@linkplain MqlNumber number} value corresponding to
     * the provided {@link Decimal128}.
     *
     * @param of the {@link Decimal128}.
     * @return the resulting value.
     */
    public static MqlNumber of(final Decimal128 of) {
        Assertions.notNull("Decimal128", of);
        return new MqlExpression<>((codecRegistry) -> new AstPlaceholder(new BsonDecimal128(of)));
    }

    /**
     * Returns an {@linkplain MqlArray array} of
     * {@linkplain MqlNumber numbers} corresponding to
     * the provided {@link Decimal128}s.
     *
     * @param array the array.
     * @return the resulting value.
     */
    public static MqlArray<MqlNumber> ofNumberArray(final Decimal128... array) {
        Assertions.notNull("array", array);
        List<BsonValue> result = new ArrayList<>();
        for (Decimal128 e : array) {
            Assertions.notNull("elements of array", e);
            result.add(new BsonDecimal128(e));
        }
        return new MqlExpression<>((cr) -> new AstPlaceholder(new BsonArray(result)));
    }

    /**
     * Returns a {@linkplain MqlDate date and time} value corresponding to
     * the provided {@link Instant}.
     *
     * @param of the {@link Instant}.
     * @return the resulting value.
     */
    public static MqlDate of(final Instant of) {
        Assertions.notNull("Instant", of);
        return new MqlExpression<>((codecRegistry) -> new AstPlaceholder(new BsonDateTime(of.toEpochMilli())));
    }

    /**
     * Returns an {@linkplain MqlArray array} of
     * {@linkplain MqlDate dates} corresponding to
     * the provided {@link Instant}s.
     *
     * @param array the array.
     * @return the resulting value.
     */
    public static MqlArray<MqlDate> ofDateArray(final Instant... array) {
        Assertions.notNull("array", array);
        List<BsonValue> result = new ArrayList<>();
        for (Instant e : array) {
            Assertions.notNull("elements of array", e);
            result.add(new BsonDateTime(e.toEpochMilli()));
        }
        return new MqlExpression<>((cr) -> new AstPlaceholder(new BsonArray(result)));
    }

    /**
     * Returns an {@linkplain MqlString string} value corresponding to
     * the provided {@link String}.
     *
     * @param of the {@link String}.
     * @return the resulting value.
     */
    public static MqlString of(final String of) {
        Assertions.notNull("String", of);
        return new MqlExpression<>((codecRegistry) -> new AstPlaceholder(wrapString(of)));
    }

    /**
     * Returns an {@linkplain MqlArray array} of
     * {@linkplain MqlString strings} corresponding to
     * the provided {@link String}s.
     *
     * @param array the array.
     * @return the resulting value.
     */
    public static MqlArray<MqlString> ofStringArray(final String... array) {
        Assertions.notNull("array", array);
        List<BsonValue> result = new ArrayList<>();
        for (String e : array) {
            Assertions.notNull("elements of array", e);
            result.add(wrapString(e));
        }
        return new MqlExpression<>((cr) -> new AstPlaceholder(new BsonArray(result)));
    }

    private static BsonValue wrapString(final String s) {
        BsonString bson = new BsonString(s);
        if (s.contains("$")) {
            return new BsonDocument("$literal", bson);
        } else {
            return bson;
        }
    }

    /**
     * Returns a reference to the "current"
     * {@linkplain MqlDocument document} value.
     * The "current" value is the top-level document currently being processed
     * in the aggregation pipeline stage.
     *
     * @return a reference to the current value
     */
    public static MqlDocument current() {
        return new MqlExpression<>((cr) -> new AstPlaceholder(new BsonString("$$CURRENT")))
                .assertImplementsAllExpressions();
    }

    /**
     * Returns a reference to the "current"
     * value as a {@linkplain MqlMap map} value.
     * The "current" value is the top-level document currently being processed
     * in the aggregation pipeline stage.
     *
     * <p>Warning: The type of the values of the resulting map are not
     * enforced by the API. The specification of a type by the user is an
     * unchecked assertion that all map values are of that type.
     * If the map contains multiple types (such as both nulls and integers)
     * then a super-type encompassing all types must be chosen, and
     * if necessary the elements should be individually type-checked when used.
     *
     * @return a reference to the current value as a map.
     * @param <R> the type of the map's values.
     */
    public static <R extends MqlValue> MqlMap<@MqlUnchecked(TYPE_ARGUMENT) R> currentAsMap() {
        return new MqlExpression<>((cr) -> new AstPlaceholder(new BsonString("$$CURRENT")))
                .assertImplementsAllExpressions();
    }

    /**
     * Returns an {@linkplain MqlDocument array} value, containing the
     * {@linkplain MqlValue values} provided.
     *
     * @param array the {@linkplain MqlValue values}.
     * @return the resulting value.
     * @param <T> the type of the array elements.
     */
    @SafeVarargs  // nothing is stored in the array
    public static <T extends MqlValue> MqlArray<T> ofArray(final T... array) {
        Assertions.notNull("array", array);
        return new MqlExpression<>((cr) -> {
            BsonArray bsonArray = new BsonArray();
            for (T v : array) {
                Assertions.notNull("elements of array", v);
                bsonArray.add(((MqlExpression<?>) v).toBsonValue(cr));
            }
            return new AstPlaceholder(bsonArray);
        });
    }

    /**
     * Returns an {@linkplain MqlEntry entry} value.
     *
     * @param k the key.
     * @param v the value.
     * @return the resulting value.
     * @param <T> the type of the key.
     */
    public static <T extends MqlValue> MqlEntry<T> ofEntry(final MqlString k, final T v) {
        Assertions.notNull("k", k);
        Assertions.notNull("v", v);
        return new MqlExpression<>((cr) -> {
            BsonDocument document = new BsonDocument();
            document.put("k", toBsonValue(cr, k));
            document.put("v", toBsonValue(cr, v));
            return new AstPlaceholder(document);
        });
    }

    /**
     * Returns an empty {@linkplain MqlMap map} value.
     *
     * @param <T> the type of the resulting map's values.
     * @return the resulting map value.
     */
    public static <T extends MqlValue> MqlMap<T> ofMap() {
        return ofMap(new BsonDocument());
    }

    /**
     * Returns a {@linkplain MqlMap map} value corresponding to the
     * provided {@link Bson Bson document}.
     *
     * <p>Warning: The type of the values of the resulting map are not
     * enforced by the API. The specification of a type by the user is an
     * unchecked assertion that all map values are of that type.
     * If the map contains multiple types (such as both nulls and integers)
     * then a super-type encompassing all types must be chosen, and
     * if necessary the elements should be individually type-checked when used.
     *
     * @param map the map as a {@link Bson Bson document}.
     * @param <T> the type of the resulting map's values.
     * @return the resulting map value.
     */
    public static <T extends MqlValue> MqlMap<@MqlUnchecked(TYPE_ARGUMENT) T> ofMap(final Bson map) {
        Assertions.notNull("map", map);
        return new MqlExpression<>((cr) -> new AstPlaceholder(new BsonDocument("$literal",
                map.toBsonDocument(BsonDocument.class, cr))));
    }

    /**
     * Returns a {@linkplain MqlDocument document} value corresponding to the
     * provided {@link Bson Bson document}.
     *
     * @param document the {@linkplain Bson BSON document}.
     * @return the resulting value.
     */
    public static MqlDocument of(final Bson document) {
        Assertions.notNull("document", document);
        // All documents are wrapped in a $literal; this is the least brittle approach.
        return new MqlExpression<>((cr) -> new AstPlaceholder(new BsonDocument("$literal",
                document.toBsonDocument(BsonDocument.class, cr))));
    }

    /**
     * The null value in the context of the MongoDB Query Language (MQL).
     *
     * <p>The null value is not part of, and cannot be used as if it were part
     * of, any explicit type (except the root type {@link MqlValue} itself).
     * It has no explicit type of its own.
     *
     * <p>Instead of checking that a value is null, users should generally
     * check that a value is of their expected type, via methods such as
     * {@link MqlValue#isNumberOr(MqlNumber)}. Where the null value
     * must be checked explicitly, users may use {@link Branches#isNull} within
     * {@link MqlValue#switchOn}.
     *
     * @return the null value
     */
    public static MqlValue ofNull() {
        // There is no specific mql type corresponding to Null,
        // and Null is not a value in any other mql type.
        return new MqlExpression<>((cr) -> new AstPlaceholder(new BsonNull()))
                .assertImplementsAllExpressions();
    }

    static MqlNumber numberToMqlNumber(final Number number) {
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
