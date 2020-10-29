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
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

@Immutable
public final class CustomExpression implements Expression {
    private final Bson expression;

    CustomExpression(final Bson expression) {
        this.expression = notNull("expression", expression);
    }

    public Bson getExpression() {
        return expression;
    }

    @Override
    public BsonValue toBsonValue(final CodecRegistry codecRegistry) {
        return expression.toBsonDocument(BsonDocument.class, codecRegistry);
    }

    @Override
    public String toString() {
        return "CustomExpression{" +
                "expression=" + expression +
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

        CustomExpression that = (CustomExpression) o;

        if (!expression.equals(that.expression)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return expression.hashCode();
    }
}
