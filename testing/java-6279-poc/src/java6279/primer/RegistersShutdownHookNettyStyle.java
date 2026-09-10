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
 * The combination: a shutdown hook, registered with Netty's context class loader nulling applied around the creation
 * of the hook thread. The hook body is still a child-loaded class, because a hook that is going to call
 * {@code disablePruning()} has to reference driver code.
 *
 * <p>Expected to remain pinned. Nulling the context class loader removes one edge; it does nothing about the
 * {@code Runnable}, and {@code ApplicationShutdownHooks} holds the hook thread — and therefore its {@code Runnable},
 * and therefore this class and its loader — in a static map until the JVM exits.</p>
 */
final class RegistersShutdownHookNettyStyle {
    static {
        Poc.log("%s is being initialized by %s", RegistersShutdownHookNettyStyle.class.getName(),
                RegistersShutdownHookNettyStyle.class.getClassLoader());
        Thread callingThread = Thread.currentThread();
        ClassLoader parentCcl = callingThread.getContextClassLoader();
        callingThread.setContextClassLoader(null);
        try {
            Thread hook = new Thread(null, new HookBody(), "java6279-shutdown-hook-netty-style", 1, false);
            hook.setContextClassLoader(null);
            Runtime.getRuntime().addShutdownHook(hook);
            Poc.log("%s registered a shutdown hook with context class loader %s",
                    RegistersShutdownHookNettyStyle.class.getName(), hook.getContextClassLoader());
        } finally {
            callingThread.setContextClassLoader(parentCcl);
        }
    }

    /** Child-loaded, as any hook that called driver code would have to be. */
    private static final class HookBody implements Runnable {
        @Override
        public void run() {
            Poc.log("the Netty-style shutdown hook ran");
        }
    }

    private RegistersShutdownHookNettyStyle() {
    }
}
