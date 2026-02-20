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

package util;

import org.opentest4j.MultipleFailuresError;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class ThreadTestHelpers {

    private ThreadTestHelpers() {
    }

    public static void executeAll(final int nThreads, final Runnable c) {
        executeAll(Collections.nCopies(nThreads, c).toArray(new Runnable[0]));
    }

    public static void executeAll(final Runnable... runnables) {
        ExecutorService service = null;
        try {
            service = Executors.newFixedThreadPool(runnables.length);
            CountDownLatch latch = new CountDownLatch(runnables.length);
            List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
            for (final Runnable runnable : runnables) {
                service.execute(() -> {
                    try {
                        runnable.run();
                    } catch (Throwable e) {
                        failures.add(e);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            if (!failures.isEmpty()) {
                MultipleFailuresError multipleFailuresError = new MultipleFailuresError("Failed to execute all", failures);
                failures.forEach(multipleFailuresError::addSuppressed);
                throw multipleFailuresError;
            }
        } finally {
            if (service != null) {
                service.shutdown();
            }
        }
    }
}
