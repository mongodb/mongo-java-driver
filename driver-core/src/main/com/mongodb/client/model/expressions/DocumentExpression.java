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

    <R extends DocumentExpression> R setField(String path, Expression exp);

    DocumentExpression unsetField(String path);


    BooleanExpression getBoolean(String field);

    BooleanExpression getBoolean(String field, BooleanExpression orElse);

    default BooleanExpression getBoolean(final String field, final boolean orElse) {
        return getBoolean(field, of(orElse));
    }

    NumberExpression getNumber(String field);

    NumberExpression getNumber(String field, NumberExpression orElse);

    default NumberExpression getNumber(final String field, final Number orElse) {
        return getNumber(field, Expressions.numberToExpression(orElse));
    }

    IntegerExpression getInteger(String field);

    StringExpression getString(String field);

    StringExpression getString(String field, StringExpression orElse);

    default StringExpression getString(final String field, final String orElse) {
        return getString(field, of(orElse));
    }

    DateExpression getDate(String field);
    DateExpression getDate(String field, DateExpression orElse);

    default DateExpression getDate(final String field, final Instant orElse) {
        return getDate(field, of(orElse));
    }

    DocumentExpression getDocument(String field);
    DocumentExpression getDocument(String field, DocumentExpression orElse);

    default DocumentExpression getDocument(final String field, final Bson orElse) {
        return getDocument(field, of(orElse));
    }

    <T extends Expression> ArrayExpression<T> getArray(String field);

    <T extends Expression> ArrayExpression<T> getArray(String field, ArrayExpression<T> orElse);

    DocumentExpression merge(DocumentExpression ofDoc);
}
