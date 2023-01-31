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

package com.mongodb.client.model.mql;

import com.mongodb.annotations.Beta;
import com.mongodb.lang.Nullable;

import java.util.List;
import java.util.function.Function;

/**
 * See {@link Branches}. This is the terminal branch, to which no additional
 * checks may be added, since the default value has been specified.
 *
 * @param <T> the type of the values that may be checked.
 * @param <R> the type of the value produced.
 * @since 4.9.0
 */
@Beta(Beta.Reason.CLIENT)
public class BranchesTerminal<T extends MqlValue, R extends MqlValue> {

    private final List<Function<T, SwitchCase<R>>> branches;

    private final Function<T, R> defaults;

    BranchesTerminal(final List<Function<T, SwitchCase<R>>> branches, @Nullable final Function<T, R> defaults) {
        this.branches = branches;
        this.defaults = defaults;
    }

    BranchesTerminal<T, R> withDefault(final Function<T, R> defaults) {
        return new BranchesTerminal<>(branches, defaults);
    }

    List<Function<T, SwitchCase<R>>> getBranches() {
        return branches;
    }

    @Nullable
    Function<T, R> getDefaults() {
        return defaults;
    }
}
