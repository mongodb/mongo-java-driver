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
 * Starts a thread whose {@code Thread} object was constructed by a parent-loaded class ({@link Poc}) rather than by
 * this class. Only the {@code start()} call happens with this class on the stack.
 *
 * <p>The original experiment noted that this variant does <em>not</em> pin the loader, which is what isolates thread
 * <em>construction</em>, rather than thread execution or the thread's context class loader, as the point at which the
 * loader is captured. The harness records this scenario's outcome without asserting it, since it is the one result
 * that plausibly varies by JVM and JDK version.</p>
 */
final class StartsParentBuiltThread {
    static {
        Poc.log("%s is being initialized by %s", StartsParentBuiltThread.class.getName(),
                StartsParentBuiltThread.class.getClassLoader());
        Thread thread = Poc.PARENT_BUILT_THREAD;
        thread.start();
        Poc.log("%s started %s with context class loader %s", StartsParentBuiltThread.class.getName(),
                thread.getName(), thread.getContextClassLoader());
    }

    private StartsParentBuiltThread() {
    }
}
