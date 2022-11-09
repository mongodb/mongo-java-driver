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

import com.mongodb.client.model.expressions.MqlExpression.MqlArrayWrappingExpression;
import com.mongodb.client.model.expressions.MqlExpression.MqlBooleanWrappingExpression;
import com.mongodb.client.model.expressions.MqlExpression.MqlIntegerWrappingExpression;
import com.mongodb.client.model.expressions.MqlExpression.MqlStringWrappingExpression;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;

import java.util.ArrayList;
import java.util.List;

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
        return new MqlBooleanWrappingExpression<>(new MqlExpression<>((codecRegistry) -> new BsonBoolean(of)));
    }

    /**
     * Returns an expression having the same integer value as the provided
     * int primitive.
     *
     * @param of the int primitive
     * @return the integer expression
     */
    public static IntegerExpression of(final int of) {
        return new MqlIntegerWrappingExpression<>(new MqlExpression<>((codecRegistry) -> new BsonInt32(of)));
    }

    /**
     * Returns an expression having the same string value as the provided
     * string.
     *
     * @param of the string
     * @return the string expression
     */
    public static StringExpression of(final String of) {
        return new MqlStringWrappingExpression<>(new MqlExpression<>((codecRegistry) -> new BsonString(of)));
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
        return new MqlArrayWrappingExpression<>(new MqlExpression<>((cr) -> new BsonArray(result)));
    }

}
