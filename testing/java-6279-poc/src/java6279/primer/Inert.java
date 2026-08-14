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

package java6279.primer;

import java6279.Poc;

/**
 * Starts nothing. Loaded in every primer scenario alongside the scenario's trigger class, so that the "pinning is
 * loader-wide, not class-specific" part of the finding is visible: when a sibling class starts a thread, this class
 * cannot be collected either.
 *
 * <p>Class {@code D} in the original experiment.</p>
 */
final class Inert {
    static {
        Poc.log("%s is being initialized by %s", Inert.class.getName(), Inert.class.getClassLoader());
    }

    private Inert() {
    }
}
