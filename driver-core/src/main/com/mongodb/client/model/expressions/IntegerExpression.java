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
 * Expresses an integer value.
 */
public interface IntegerExpression extends NumberExpression {
    IntegerExpression multiply(IntegerExpression i);

    default IntegerExpression multiply(final int multiply) {
        return this.multiply(Expressions.of(multiply));
    }

    IntegerExpression add(IntegerExpression i);

    default IntegerExpression add(final int add) {
        return this.add(Expressions.of(add));
    }

    IntegerExpression subtract(IntegerExpression i);

    default IntegerExpression subtract(final int subtract) {
        return this.subtract(Expressions.of(subtract));
    }

    IntegerExpression max(IntegerExpression i);
    IntegerExpression min(IntegerExpression i);

    IntegerExpression abs();
}
