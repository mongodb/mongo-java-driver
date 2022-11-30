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

import org.bson.conversions.Bson;

import java.time.Instant;

import static com.mongodb.client.model.expressions.Expressions.of;

/**
 * Expresses a document value. A document is an ordered set of fields, where the
 * key is a string value, mapping to a value of any other expression type.
 */
public interface DocumentExpression extends Expression {

    DocumentExpression setField(String fieldName, Expression exp);

    DocumentExpression unsetField(String fieldName);


    BooleanExpression getBoolean(String fieldName);

    BooleanExpression getBoolean(String fieldName, BooleanExpression other);

    default BooleanExpression getBoolean(final String fieldName, final boolean other) {
        return getBoolean(fieldName, of(other));
    }

    NumberExpression getNumber(String fieldName);

    NumberExpression getNumber(String fieldName, NumberExpression other);

    default NumberExpression getNumber(final String fieldName, final Number other) {
        return getNumber(fieldName, Expressions.numberToExpression(other));
    }

    IntegerExpression getInteger(String fieldName);

    IntegerExpression getInteger(String fieldName, IntegerExpression other);

    default IntegerExpression getInteger(final String fieldName, final int other) {
        return getInteger(fieldName, of(other));
    }

    default IntegerExpression getInteger(final String fieldName, final long other) {
        return getInteger(fieldName, of(other));
    }


    StringExpression getString(String fieldName);

    StringExpression getString(String fieldName, StringExpression other);

    default StringExpression getString(final String fieldName, final String other) {
        return getString(fieldName, of(other));
    }

    DateExpression getDate(String fieldName);
    DateExpression getDate(String fieldName, DateExpression other);

    default DateExpression getDate(final String fieldName, final Instant other) {
        return getDate(fieldName, of(other));
    }

    DocumentExpression getDocument(String fieldName);
    DocumentExpression getDocument(String fieldName, DocumentExpression other);

    default DocumentExpression getDocument(final String fieldName, final Bson other) {
        return getDocument(fieldName, of(other));
    }

    <T extends Expression> ArrayExpression<T> getArray(String fieldName);

    <T extends Expression> ArrayExpression<T> getArray(String fieldName, ArrayExpression<? extends T> other);

    DocumentExpression merge(DocumentExpression other);
}
