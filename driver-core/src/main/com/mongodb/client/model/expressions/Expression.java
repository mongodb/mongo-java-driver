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

import com.mongodb.lang.Nullable;

import java.util.function.BiFunction;
import java.util.function.Function;

// TODO: notice to users that this should be treated as sealed.
/**
 *
 */
public interface Expression {

    BooleanExpression eq(Expression eq);

    BooleanExpression gt(Expression gt);


    <T extends Expression> T ifNull(T ifNull);

    <T extends Expression, R extends Expression> R dot(Function<T, R> f);

    <
            T extends Expression,
            R extends Expression,
            TV1 extends Expression> T let(
                    TV1 var1,
                    BiFunction<T, TV1, R> ex);

    @Nullable // TODO
    <T0 extends Expression, R0 extends Expression> T0 switchMap(
            BiFunction<T0, OptionalExpression, R0> switchMap);

    class OptionalExpression {

        public OptionalExpression caseEq(final BooleanExpression fal, final StringExpression then1) {
            return this;
        }

        @Nullable // TODO
        public Expression defaults(final StringExpression else0) {
            return null;
        }
    }
}
