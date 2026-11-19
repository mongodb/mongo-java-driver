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
 * The only shutdown hook shape that does not pin: the hook thread is CONSTRUCTED by a parent-loaded class, not
 * merely bodied by one. Registering a hook thread that this class constructs pins the loader even with a
 * parent-loaded {@code Runnable} and a null context class loader — measured, and consistent with
 * {@code StartsOwnThread} versus {@code StartsParentBuiltThread}.
 *
 * <p>Expected to be collected — and to be useless, which is the point. A hook that references nothing in the driver's
 * class loader cannot call driver code, so it cannot stop the pruner. The two properties are in direct tension: the
 * hook pins exactly to the extent that it is capable of doing its job.</p>
 */
final class RegistersShutdownHookParentBody {
    static {
        Poc.log("%s is being initialized by %s", RegistersShutdownHookParentBody.class.getName(),
                RegistersShutdownHookParentBody.class.getClassLoader());
        Thread callingThread = Thread.currentThread();
        ClassLoader parentCcl = callingThread.getContextClassLoader();
        callingThread.setContextClassLoader(null);
        try {
            // Both CONSTRUCTED and bodied by a parent-loaded class. Constructing it here instead -- even with a
            // parent-loaded Runnable and a null context class loader -- pins the loader, because thread construction
            // captures the constructing class (see StartsOwnThread vs StartsParentBuiltThread).
            Runtime.getRuntime().addShutdownHook(Poc.PARENT_BUILT_HOOK_THREAD);
            Poc.log("%s registered a fully parent-built shutdown hook",
                    RegistersShutdownHookParentBody.class.getName());
        } finally {
            callingThread.setContextClassLoader(parentCcl);
        }
    }

    private RegistersShutdownHookParentBody() {
    }
}
