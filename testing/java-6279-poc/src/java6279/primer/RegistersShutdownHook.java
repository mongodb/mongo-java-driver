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
 * Registers a JVM shutdown hook that would stop the pruner, and starts no thread of its own.
 *
 * <p>This checks the other tempting fix: leave the pruner thread running and shut it down from a
 * {@link Runtime#addShutdownHook(Thread)} hook. The hook body is a child-loaded class, as a real one would be, since
 * the whole point would be to call driver code.</p>
 *
 * <p>Expected to be worse than useless. {@code ApplicationShutdownHooks} keeps registered hooks in a static map held
 * by a bootstrap-loaded class, so the hook thread — and through it its {@code Runnable}, this class, and this class
 * loader — is strongly reachable until the JVM exits. Registering the hook therefore <em>creates</em> a permanent pin
 * in a class that otherwise had none.</p>
 */
final class RegistersShutdownHook {
    static {
        Poc.log("%s is being initialized by %s", RegistersShutdownHook.class.getName(),
                RegistersShutdownHook.class.getClassLoader());
        // The Runnable is an instance of this child-loaded class, exactly as a hook that called driver code would be.
        Runtime.getRuntime().addShutdownHook(new Thread(new HookBody(), "java6279-shutdown-hook"));
        Poc.log("%s registered a shutdown hook and started no thread of its own",
                RegistersShutdownHook.class.getName());
    }

    private static final class HookBody implements Runnable {
        @Override
        public void run() {
            Poc.log("the shutdown hook ran");
        }
    }

    private RegistersShutdownHook() {
    }
}
