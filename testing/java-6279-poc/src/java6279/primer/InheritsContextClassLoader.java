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
 * Isolates the <em>context class loader</em> edge, which is the one Netty's mitigation -- and the
 * {@code DaemonThreadFactory.newThread} change of the same shape -- actually addresses.
 *
 * <p>Models a driver thread created while an application's class loader is current: the thread is constructed by a
 * parent-loaded class (so there is no construction-site pin, per {@code StartsParentBuiltThread}), but the calling
 * thread's context class loader is this child loader, so the new thread inherits it.</p>
 *
 * <p>This is the inverse of JAVA-6279's own problem: the loader at risk here is the <em>application's</em>, pinned by a
 * driver thread, rather than the driver's own.</p>
 */
final class InheritsContextClassLoader {
    static {
        Poc.log("%s is being initialized by %s", InheritsContextClassLoader.class.getName(), InheritsContextClassLoader.class.getClassLoader());
        Thread callingThread = Thread.currentThread();
        ClassLoader previous = callingThread.getContextClassLoader();
        // Pretend an application thread with its own class loader is the one calling into the driver.
        callingThread.setContextClassLoader(InheritsContextClassLoader.class.getClassLoader());
        try {
            Poc.buildAndStartThreadInheritingCcl(false);
        } finally {
            callingThread.setContextClassLoader(previous);
        }
    }

    private InheritsContextClassLoader() {
    }
}
