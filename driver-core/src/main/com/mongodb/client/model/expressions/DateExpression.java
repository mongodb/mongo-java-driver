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
 * Expresses a date value.
 */
public interface DateExpression extends Expression {
    IntegerExpression year(StringExpression timezone);
    IntegerExpression month(StringExpression timezone);
    IntegerExpression dayOfMonth(StringExpression timezone);
    IntegerExpression dayOfWeek(StringExpression timezone);
    IntegerExpression dayOfYear(StringExpression timezone);
    IntegerExpression hour(StringExpression timezone);
    IntegerExpression minute(StringExpression timezone);
    IntegerExpression second(StringExpression timezone);
    IntegerExpression week(StringExpression timezone);
    IntegerExpression millisecond(StringExpression timezone);

    StringExpression asString(StringExpression timezone, StringExpression format);

    <R extends Expression> R passDateTo(Function<? super DateExpression, R> f);
    <R extends Expression> R switchDateOn(Function<Branches, ? extends BranchesTerminal<? super DateExpression, R>> on);
}
