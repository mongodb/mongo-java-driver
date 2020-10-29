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

import jdk.nashorn.internal.ir.annotations.Immutable;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Objects;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.client.model.expressions.ExpressionHelper.toBsonArray;
import static java.util.Arrays.asList;

@Immutable
public class GreaterThanOrEqualExpression implements Expression {
    private final Expression first;
    private final Expression second;

    GreaterThanOrEqualExpression(final Expression first, final Expression second) {
        this.first = notNull("first", first);
        this.second = notNull("second", second);
    }

    public Expression getFirst() {
        return first;
    }

    public Expression getSecond() {
        return second;
    }

    @Override
    public BsonValue toBsonValue(final CodecRegistry codecRegistry) {
        return new BsonDocument("$gte", toBsonArray(asList(first, second), codecRegistry));
    }

    @Override
    public String toString() {
        return "GreaterThanOrEqualExpression{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GreaterThanOrEqualExpression that = (GreaterThanOrEqualExpression) o;
        return Objects.equals(first, that.first) &&
                Objects.equals(second, that.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }
}
