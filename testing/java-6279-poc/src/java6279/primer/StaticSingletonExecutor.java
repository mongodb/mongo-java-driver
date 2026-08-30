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

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Models {@code com.mongodb.internal.thread.CommonExecutor} from the backpressure work
 * (<a href="https://github.com/stIncMale/mongo-java-driver/pull/3">stIncMale PR 3</a>, JAVA-6240): a static singleton
 * holding a {@code ScheduledThreadPoolExecutor} that nothing ever shuts down.
 *
 * <p>Also registers the {@link java.lang.ref.Cleaner} that {@code CommonExecutor}'s {@code VAKOTODO} proposes as the
 * eventual fix — "use Cleaner when we are at Java SE 17 to shut down internal executors if the class is GCed" — so
 * that the proposal can be checked rather than assumed. The cleaning action holds no reference to the singleton, as
 * {@code Cleaner} requires.</p>
 *
 * <p>The expectation is that the cleaning action never runs, because the reachability is circular: the singleton is
 * reachable from its class, the class from its class loader, and the loader is pinned by the very thread the cleaning
 * action was supposed to stop.</p>
 */
final class StaticSingletonExecutor {
    /** As {@code CommonExecutor.INSTANCE} is: a static field, so reachable for as long as the class is loaded. */
    private static final StaticSingletonExecutor INSTANCE = new StaticSingletonExecutor();

    private final ScheduledThreadPoolExecutor singleThreadScheduler;

    static {
        Poc.log("%s is being initialized by %s", StaticSingletonExecutor.class.getName(),
                StaticSingletonExecutor.class.getClassLoader());
        // Capture the executor, not the singleton: a cleaning action that referenced INSTANCE would pin it by itself
        // and the check would prove nothing.
        final ScheduledThreadPoolExecutor executor = INSTANCE.singleThreadScheduler;
        Poc.registerCleaner(INSTANCE, () -> {
            Poc.log("the cleaning action ran; shutting the executor down");
            executor.shutdownNow();
        });
        // CommonExecutor starts its thread lazily, on the first schedule() call. Model that.
        INSTANCE.singleThreadScheduler.scheduleAtFixedRate(
                () -> { }, 0, 50, TimeUnit.MILLISECONDS);
        Poc.log("%s scheduled a task, starting the CommonScheduler-equivalent thread",
                StaticSingletonExecutor.class.getName());
    }

    private StaticSingletonExecutor() {
        singleThreadScheduler = new ScheduledThreadPoolExecutor(1,
                runnable -> {
                    Thread thread = new Thread(runnable, "java6279-CommonScheduler");
                    thread.setDaemon(true);
                    return thread;
                });
        singleThreadScheduler.setRemoveOnCancelPolicy(true);
    }
}
