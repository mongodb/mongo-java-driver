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

public interface DocumentExpression extends Expression {

    BooleanExpression getFieldBoolean(String field);

    NumberExpression getFieldNumber(String field);

    IntegerExpression getFieldInteger(String field);

    StringExpression getFieldString(String field);

    DateExpression getFieldDate(String field);

    DocumentExpression getFieldDocument(String field);

    <T extends DocumentExpression> ArrayExpression<T> getFieldArray(String field);

    // TODO and getFieldIntegerArrayArrayArray?
    ArrayExpression<DocumentExpression> getFieldDocumentArray(String field);

    <R extends DocumentExpression> R setField(String path, Expression exp);

    DocumentExpression removeField(String path);

    // TODO all document/array methods (maybe all) need to return T extends Type to allow for "schema" classes
}
