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

import java.util.List;
import java.util.function.Function;

public class BranchesTerminal<T extends Expression, R extends Expression> {

    private final List<Function<T, SwitchCase<R>>> branches;

    private final Function<T, R> defaults;

    BranchesTerminal(final List<Function<T, SwitchCase<R>>> branches, @Nullable final Function<T, R> defaults) {
        this.branches = branches;
        this.defaults = defaults;
    }

    protected BranchesTerminal<T, R> withDefault(final Function<T, R> defaults) {
        return new BranchesTerminal<>(branches, defaults);
    }

    protected List<Function<T, SwitchCase<R>>> getBranches() {
        return branches;
    }

    @Nullable
    protected Function<T, R> getDefaults() {
        return defaults;
    }
}
