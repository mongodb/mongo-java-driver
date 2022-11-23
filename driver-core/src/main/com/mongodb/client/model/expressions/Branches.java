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

    Branches() {
        // package-private
    }

    private <T extends Expression, R extends Expression> BranchesIntermediary<T, R> with(final Function<T, SwitchCase<R>> switchCase) {
        List<Function<T, SwitchCase<R>>> v = new ArrayList<>();
        v.add(switchCase);
        return new BranchesIntermediary<>(v);
    }

    private static <T extends Expression> MqlExpression<?> mqlEx(final T value) {
        return (MqlExpression<?>) value;
    }

    // is fn

    public <T extends Expression, R extends Expression> BranchesIntermediary<T, R> is(final Function<T, BooleanExpression> o, final Function<T, R> r) {
        return this.with(value -> new SwitchCase<>(o.apply(value), r.apply(value)));
    }

    // eq lt lte

    public <T extends Expression, R extends Expression> BranchesIntermediary<T, R> eq(final T v, final Function<T, R> r) {
        return this.with(value -> new SwitchCase<>(value.eq(v), r.apply(value)));
    }

    public <T extends Expression, R extends Expression> BranchesIntermediary<T, R> lt(final T v, final Function<T, R> r) {
        return this.with(value -> new SwitchCase<>(value.lt(v), r.apply(value)));
    }

    public <T extends Expression, R extends Expression> BranchesIntermediary<T, R> lte(final T v, final Function<T, R> r) {
        return this.with(value -> new SwitchCase<>(value.lte(v), r.apply(value)));
    }

    // is type

    public <T extends Expression, R extends Expression> BranchesIntermediary<T, R> isBoolean(final Function<BooleanExpression, R> r) {
        return this.with(value -> new SwitchCase<>(mqlEx(value).isBoolean(), r.apply((BooleanExpression) value)));
    }

    public <T extends Expression, R extends Expression> BranchesIntermediary<T, R> isNumber(final Function<NumberExpression, R> r) {
        return this.with(value -> new SwitchCase<>(mqlEx(value).isNumber(), r.apply((NumberExpression) value)));
    }

    public <T extends Expression, R extends Expression> BranchesIntermediary<T, R> isString(final Function<StringExpression, R> r) {
        return this.with(value -> new SwitchCase<>(mqlEx(value).isString(), r.apply((StringExpression) value)));
    }

    public <T extends Expression, R extends Expression> BranchesIntermediary<T, R> isDate(final Function<DateExpression, R> r) {
        return this.with(value -> new SwitchCase<>(mqlEx(value).isDate(), r.apply((DateExpression) value)));
    }

    public <T extends Expression, R extends Expression, Q extends Expression> BranchesIntermediary<T, R> isArray(final Function<ArrayExpression<Q>, R> r) {
        return this.with(value -> new SwitchCase<>(mqlEx(value).isArray(), r.apply((ArrayExpression<Q>) value)));
    }

    public <T extends Expression, R extends Expression> BranchesIntermediary<T, R> isDocument(final Function<DocumentExpression, R> r) {
        return this.with(value -> new SwitchCase<>(mqlEx(value).isDocument(), r.apply((DocumentExpression) value)));
    }

    public <T extends Expression, R extends Expression, Q extends Expression> BranchesIntermediary<T, R> isMap(final Function<MapExpression<Q>, R> r) {
        return this.with(value -> new SwitchCase<>(mqlEx(value).isDocument(), r.apply((MapExpression<Q>) value)));
    }

    public <T extends Expression, R extends Expression> BranchesIntermediary<T, R> isNull(final Function<Expression, R> isNull) {
        return this.with(value -> new SwitchCase<>(mqlEx(value).isNull(), isNull.apply(value)));
    }
}
