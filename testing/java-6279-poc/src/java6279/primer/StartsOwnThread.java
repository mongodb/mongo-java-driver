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
 * Constructs and starts a thread from its own static initializer. This is the shape of the driver's own code: a
 * class in the driver's class loader creates a thread that outlives the work that prompted it.
 *
 * <p>The {@code Runnable} comes from {@link Poc}, which the primer class loader delegates to its parent, so the
 * running thread holds no reference into this class loader by way of its task. The thread is nonetheless expected to
 * keep this class loader strongly reachable.</p>
 *
 * <p>Class {@code C} in the original experiment, in its {@code new Thread(null, runnable, name, 1, false)} form.</p>
 */
final class StartsOwnThread {
    static {
        Poc.log("%s is being initialized by %s", StartsOwnThread.class.getName(),
                StartsOwnThread.class.getClassLoader());
        // The 5-argument constructor is the one the original experiment found sufficient to pin the loader: it takes
        // no thread group and does not inherit thread locals, so the pinning cannot be explained by inherited state.
        Thread thread = new Thread(null, Poc.SLEEPING_RUNNABLE, "java6279-own-thread", 1, false);
        thread.start();
        Poc.log("%s started %s with context class loader %s", StartsOwnThread.class.getName(), thread.getName(),
                thread.getContextClassLoader());
    }

    private StartsOwnThread() {
    }
}
