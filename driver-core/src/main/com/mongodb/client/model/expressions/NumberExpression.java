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

import java.util.function.Function;

/**
 * Expresses a numeric value.
 */
public interface NumberExpression extends Expression {

    NumberExpression multiply(NumberExpression n);

    default NumberExpression multiply(final Number multiply) {
        return this.multiply(Expressions.numberToExpression(multiply));
    }

    NumberExpression divide(NumberExpression n);

    default NumberExpression divide(final Number divide) {
        return this.divide(Expressions.numberToExpression(divide));
    }

    NumberExpression add(NumberExpression n);

    default NumberExpression add(final Number add) {
        return this.add(Expressions.numberToExpression(add));
    }

    NumberExpression subtract(NumberExpression n);

    default NumberExpression subtract(final Number subtract) {
        return this.subtract(Expressions.numberToExpression(subtract));
    }

    NumberExpression max(NumberExpression n);

    NumberExpression min(NumberExpression n);

    IntegerExpression round();

    NumberExpression round(IntegerExpression place);

    NumberExpression abs();

    DateExpression millisecondsToDate();

    <R extends Expression> R passNumberTo(Function<? super NumberExpression, R> f);
    <R extends Expression> R switchNumberOn(Function<Branches, ? extends BranchesTerminal<? super NumberExpression, R>> on);
}
