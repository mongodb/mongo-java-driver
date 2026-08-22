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

package com.mongodb.internal.event;

import com.mongodb.ServerAddress;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.client.FailPoint;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import org.bson.BsonDocument;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.fail;

@ThreadSafe
public final class ConfigureFailPointCommandListener implements CommandListener, AutoCloseable {
    private final BsonDocument configureFailPoint;
    private final ServerAddress serverAddress;
    private final Predicate<CommandEvent> eventMatcher;
    private final Object lock;
    private final CompletableFuture<FailPoint> failPointFuture;

    /**
     * @param configureFailPoint See {@link FailPoint#enable(BsonDocument, ServerAddress)}.
     * @param serverAddress See {@link FailPoint#enable(BsonDocument, ServerAddress)}.
     * @param eventMatcher When an event is matched, an attempt to configure the fail point
     * specified via {@code configureFailPoint} is made.
     * The {@code eventMatcher} is guaranteed to be {@linkplain Predicate#test(Object) used} sequentially.
     * The attempt is made at most once,
     * and the {@code eventMatcher} {@linkplain Predicate#test(Object) test} that caused the attempt is the last one.
     */
    public ConfigureFailPointCommandListener(
            final BsonDocument configureFailPoint,
            final ServerAddress serverAddress,
            final Predicate<CommandEvent> eventMatcher) {
        this.configureFailPoint = configureFailPoint;
        this.serverAddress = serverAddress;
        this.eventMatcher = eventMatcher;
        lock = new Object();
        failPointFuture = new CompletableFuture<>();
    }

    @Override
    public void commandStarted(final CommandStartedEvent event) {
        onEvent(event);
    }

    @Override
    public void commandSucceeded(final CommandSucceededEvent event) {
        onEvent(event);
    }

    @Override
    public void commandFailed(final CommandFailedEvent event) {
        onEvent(event);
    }

    private void onEvent(final CommandEvent event) {
        synchronized (lock) {
            if (!failPointFuture.isDone()) {
                try {
                    if (eventMatcher.test(event)) {
                        assertTrue(failPointFuture.complete(FailPoint.enable(configureFailPoint, serverAddress)));
                    }
                } catch (Throwable e) {
                    assertTrue(failPointFuture.completeExceptionally(e));
                }
            }
        }
    }

    @Override
    public void close() throws InterruptedException, ExecutionException {
        synchronized (lock) {
            if (failPointFuture.cancel(true)) {
                fail("The listener was closed before (in the happens-before order) it attempted to configure the fail point");
            } else {
                assertTrue(failPointFuture.isDone());
                assertNotNull(failPointFuture.get()).close();
            }
        }
    }
}
