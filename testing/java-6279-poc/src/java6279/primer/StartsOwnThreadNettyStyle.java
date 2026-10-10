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
 * {@link StartsOwnThread}, but applying Netty's class loader mitigation from
 * {@code GlobalEventExecutor.startThread()}: null the creating thread's context class loader around the
 * {@code new Thread(...)} call, null the new thread's context class loader, then restore.
 *
 * <p>Netty does this citing <a href="https://github.com/netty/netty/issues/7290">netty#7290</a> and
 * <a href="https://bugs.openjdk.org/browse/JDK-7008595">JDK-7008595</a>, with the comment "Avoid calling classloader
 * leaking through Thread.inheritedAccessControlContext".</p>
 *
 * <p>The question this scenario answers: does that mitigation also release <em>our</em> case — a thread whose own
 * class lives in the loader we want collected — or does it only address the different edge Netty cares about, a
 * long-lived global thread pinning whichever application class loader happened to be current when it started?</p>
 */
final class StartsOwnThreadNettyStyle {
    static {
        Poc.log("%s is being initialized by %s", StartsOwnThreadNettyStyle.class.getName(),
                StartsOwnThreadNettyStyle.class.getClassLoader());
        Thread callingThread = Thread.currentThread();
        ClassLoader parentCcl = callingThread.getContextClassLoader();
        callingThread.setContextClassLoader(null);
        try {
            Thread thread = new Thread(null, Poc.SLEEPING_RUNNABLE, "java6279-netty-style", 1, false);
            thread.setContextClassLoader(null);
            thread.start();
            Poc.log("%s started %s with context class loader %s", StartsOwnThreadNettyStyle.class.getName(),
                    thread.getName(), thread.getContextClassLoader());
        } finally {
            callingThread.setContextClassLoader(parentCcl);
        }
    }

    private StartsOwnThreadNettyStyle() {
    }
}
