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

import static com.mongodb.client.model.expressions.Expressions.of;

/**
 * Expresses a string value.
 */
public interface StringExpression extends Expression {

    StringExpression toLower();

    StringExpression toUpper();

    StringExpression concat(StringExpression concat);

    IntegerExpression strLen();

    IntegerExpression strLenBytes();

    StringExpression substr(IntegerExpression start, IntegerExpression length);

    default StringExpression substr(final int start, final int length) {
        return this.substr(of(start), of(length));
    }

    StringExpression substrBytes(IntegerExpression start, IntegerExpression length);

    default StringExpression substrBytes(final int start, final int length) {
        return this.substrBytes(of(start), of(length));
    }

    IntegerExpression parseInteger();

    DateExpression parseDate();

    DateExpression parseDate(StringExpression format);

    DateExpression parseDate(StringExpression timezone, StringExpression format);

    <R extends Expression> R passStringTo(Function<? super StringExpression, ? extends R> f);
    <R extends Expression> R switchStringOn(Function<Branches<StringExpression>, ? extends BranchesTerminal<StringExpression, ? extends R>> on);
}
