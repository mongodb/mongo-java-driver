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
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Convenience methods related to {@link Expression}.
 */
public final class Expressions {

    private Expressions() {}

    /**
     * Returns an expression having the same boolean value as the provided
     * boolean primitive.
     *
     * @param of the boolean primitive
     * @return the boolean expression
     */
    public static BooleanExpression of(final boolean of) {
        // we intentionally disallow ofBoolean(null)
        return new MqlExpression<>((codecRegistry) -> new BsonBoolean(of));
    }

    /**
     * Returns an expression having the same integer value as the provided
     * int primitive.
     *
     * @param of the int primitive
     * @return the integer expression
     */
    public static IntegerExpression of(final int of) {
        return new MqlExpression<>((codecRegistry) -> new BsonInt32(of));
    }
    public static IntegerExpression of(final long of) {
        return new MqlExpression<>((codecRegistry) -> new BsonInt64(of));
    }
    public static NumberExpression of(final double of) {
        return new MqlExpression<>((codecRegistry) -> new BsonDouble(of));
    }
    public static DateExpression of(final Instant of) {
        return new MqlExpression<>((codecRegistry) -> new BsonDateTime(of.toEpochMilli()));
    }

    /**
     * Returns an expression having the same string value as the provided
     * string.
     *
     * @param of the string
     * @return the string expression
     */
    public static StringExpression of(final String of) {
        return new MqlExpression<>((codecRegistry) -> new BsonString(of));
    }

    /**
     * Returns an array expression containing the same boolean values as the
     * provided array of booleans.
     *
     * @param array the array of booleans
     * @return the boolean array expression
     */
    public static ArrayExpression<BooleanExpression> ofBooleanArray(final boolean... array) {
        List<BsonValue> result = new ArrayList<>();
        for (boolean b : array) {
            result.add(new BsonBoolean(b));
        }
        return new MqlExpression<>((cr) -> new BsonArray(result));
    }


    public static ArrayExpression<IntegerExpression> ofIntegerArray(final int... ofIntegerArray) {
        List<BsonValue> array = Arrays.stream(ofIntegerArray)
                .mapToObj(BsonInt32::new)
                .collect(Collectors.toList());
        return new MqlExpression<>((cr) -> new BsonArray(array));
    }

    public static DocumentExpression ofDocument(final Bson document) {
        Assertions.notNull("document", document);
        // All documents are wrapped in a $literal. It is possible to check
        // for empty docs (see https://jira.mongodb.org/browse/SERVER-46422)
        // and wrap them, and also to traverse into each document to see if
        // it needs to be conditionally wrapped in a $literal, but doing so
        // would be brittle and unsafe - we might miss some future case and
        // allow an expression where a literal document was expected.
        return new MqlExpression<>((cr) -> new BsonDocument("$literal",
                document.toBsonDocument(BsonDocument.class, cr)));
    }

    public static <R extends Expression> R ofNull() {
        return new MqlExpression<>((cr) -> new BsonNull()).assertImplementsAllExpressions();
    }
}
