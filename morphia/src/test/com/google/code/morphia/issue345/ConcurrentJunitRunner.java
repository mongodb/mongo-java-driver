/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package com.google.code.morphia.issue345;

import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerScheduler;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentJunitRunner extends BlockJUnit4ClassRunner {

    public ConcurrentJunitRunner(final Class<?> klass) throws InitializationError {
        super(klass);
        setScheduler(new RunnerScheduler() {
            private final ExecutorService executorService = Executors.newFixedThreadPool(
                    klass.isAnnotationPresent(Concurrent.class) ? klass.getAnnotation(
                            Concurrent.class).threads() : (int) (Runtime.getRuntime()
                            .availableProcessors() * 1.5),
                    new NamedThreadFactory(klass.getSimpleName()));
            private final CompletionService<Void> completionService = new ExecutorCompletionService<Void>(
                    executorService);
            private final Queue<Future<Void>> tasks = new LinkedList<Future<Void>>();

            public void schedule(final Runnable childStatement) {
                tasks.offer(completionService.submit(childStatement, null));
            }

            public void finished() {
                try {
                    while (!tasks.isEmpty()) {
                        tasks.remove(completionService.take());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    while (!tasks.isEmpty()) {
                        tasks.poll().cancel(true);
                    }
                    executorService.shutdownNow();
                }
            }
        });
    }

    static final class NamedThreadFactory implements ThreadFactory {
        private static final AtomicInteger POOL_NUMBER = new AtomicInteger(1);
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final ThreadGroup group;

        NamedThreadFactory(final String poolName) {
            group = new ThreadGroup(poolName + "-" + POOL_NUMBER.getAndIncrement());
        }

        public Thread newThread(final Runnable r) {
            return new Thread(group, r, group.getName() + "-thread-"
                    + threadNumber.getAndIncrement(), 0);
        }
    }
}
