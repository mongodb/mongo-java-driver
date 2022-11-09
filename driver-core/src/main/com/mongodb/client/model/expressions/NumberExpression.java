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

/**
 * Expresses a numeric value.
 */
public interface NumberExpression extends Expression {

    NumberExpression multiply(NumberExpression n);

    default NumberExpression multiply(final double multiply) {
        return this.multiply(Expressions.of(multiply));
    }

    NumberExpression divide(NumberExpression n);

    default NumberExpression divide(final double divide) {
        return this.divide(Expressions.of(divide));
    }

    NumberExpression add(NumberExpression n);

    default NumberExpression add(final double add) {
        return this.add(Expressions.of(add));
    }

    NumberExpression subtract(NumberExpression n);

    default NumberExpression subtract(final double subtract) {
        return this.subtract(Expressions.of(subtract));
    }

    NumberExpression max(NumberExpression n);

    NumberExpression min(NumberExpression n);

    IntegerExpression round();

    NumberExpression round(IntegerExpression place);

    NumberExpression abs();
}
