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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public final class Branches {

    static final Branches EMPTY = new Branches();

    private Branches() {
    }

    private static <T extends Expression, R extends Expression> BranchesIntermediary<T, R> with(final Function<T, SwitchCase<R>> switchCase) {
        List<Function<T, SwitchCase<R>>> v = new ArrayList<>();
        v.add(switchCase);
        return new BranchesIntermediary<>(v);
    }

    private static <T extends Expression> MqlExpression<?> mqlEx(final T value) {
        return (MqlExpression<?>) value;
    }

    // is fn

    public <T extends Expression, R extends Expression> BranchesIntermediary<T, R> is(final Function<? super T, BooleanExpression> o, final Function<? super T, ? extends R> r) {
        return with(value -> new SwitchCase<>(o.apply(value), r.apply(value)));
    }

    // eq lt lte

    public <T extends Expression, R extends Expression> BranchesIntermediary<T, R> eq(final T v, final Function<? super T, ? extends R> r) {
        return is(value -> value.eq(v), r);
    }

    public <T extends Expression, R extends Expression> BranchesIntermediary<T, R> lt(final T v, final Function<? super T, ? extends R> r) {
        return is(value -> value.lt(v), r);
    }

    public <T extends Expression, R extends Expression> BranchesIntermediary<T, R> lte(final T v, final Function<? super T, ? extends R> r) {
        return is(value -> value.lte(v), r);
    }

    // is type

    public <T extends Expression, R extends Expression> BranchesIntermediary<T, R> isBoolean(final Function<BooleanExpression, ? extends R> r) {
        return is(v -> mqlEx(v).isBoolean(), v -> r.apply((BooleanExpression) v));
    }

    public <T extends Expression, R extends Expression> BranchesIntermediary<T, R> isNumber(final Function<NumberExpression, ? extends R> r) {
        return is(v -> mqlEx(v).isNumber(), v -> r.apply((NumberExpression) v));
    }

    public <T extends Expression, R extends Expression> BranchesIntermediary<T, R> isInteger(final Function<IntegerExpression, ? extends R> r) {
        return is(v -> mqlEx(v).isInteger(), v -> r.apply((IntegerExpression) v));
    }

    public <T extends Expression, R extends Expression> BranchesIntermediary<T, R> isString(final Function<StringExpression, ? extends R> r) {
        return is(v -> mqlEx(v).isString(), v -> r.apply((StringExpression) v));
    }

    public <T extends Expression, R extends Expression> BranchesIntermediary<T, R> isDate(final Function<DateExpression, ? extends R> r) {
        return is(v -> mqlEx(v).isDate(), v -> r.apply((DateExpression) v));
    }

    @SuppressWarnings("unchecked")
    public <T extends Expression, R extends Expression, Q extends Expression> BranchesIntermediary<T, R> isArray(final Function<ArrayExpression<Q>, ? extends R> r) {
        return is(v -> mqlEx(v).isArray(), v -> r.apply((ArrayExpression<Q>) v));
    }

    public <T extends Expression, R extends Expression> BranchesIntermediary<T, R> isDocument(final Function<DocumentExpression, ? extends R> r) {
        return is(v -> mqlEx(v).isDocument(), v -> r.apply((DocumentExpression) v));
    }

    @SuppressWarnings("unchecked")
    public <T extends Expression, R extends Expression, Q extends Expression> BranchesIntermediary<T, R> isMap(final Function<MapExpression<Q>, ? extends R> r) {
        return is(v -> mqlEx(v).isMap(), v -> r.apply((MapExpression<Q>) v));
    }

    public <T extends Expression, R extends Expression> BranchesIntermediary<T, R> isNull(final Function<Expression, ? extends R> r) {
        return is(v -> mqlEx(v).isNull(), v -> r.apply(v));
    }
}
