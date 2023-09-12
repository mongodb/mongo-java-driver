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
package com.mongodb.internal.thread;

import com.mongodb.MongoInterruptedException;
import com.mongodb.lang.Nullable;

import java.io.InterruptedIOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.util.Optional;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class InterruptionUtil {
    /**
     * {@linkplain Thread#interrupt() Interrupts} the {@linkplain Thread#currentThread() current thread}
     * before creating {@linkplain MongoInterruptedException}.
     * We do this because the interrupt status is cleared before throwing {@link InterruptedException},
     * we are not propagating {@link InterruptedException}, which means we must reinstate the interrupt status.
     * This matches the behavior documented by {@link MongoInterruptedException}.
     */
    public static MongoInterruptedException interruptAndCreateMongoInterruptedException(
            @Nullable final String msg, @Nullable final InterruptedException cause) {
        Thread.currentThread().interrupt();
        return new MongoInterruptedException(msg, cause);
    }

    /**
     * If {@code e} is {@link InterruptedException}, then {@link #interruptAndCreateMongoInterruptedException(String, InterruptedException)}
     * is used.
     *
     * @return {@link Optional#empty()} iff {@code e} does not communicate an interrupt.
     */
    public static Optional<MongoInterruptedException> translateInterruptedException(
            @Nullable final Throwable e, @Nullable final String message) {
        if (e instanceof InterruptedException) {
            return Optional.of(interruptAndCreateMongoInterruptedException(message, (InterruptedException) e));
        } else if (
                // `InterruptedIOException` is weirdly documented, and almost seems to be a relic abandoned by the Java SE APIs:
                // - `SocketTimeoutException` is `InterruptedIOException`,
                //   but it is not related to the Java SE interrupt mechanism. As a side note, it does not happen when writing.
                // - Java SE methods, where IO may indeed be interrupted via the Java SE interrupt mechanism,
                //   use different exceptions, like `ClosedByInterruptException` or even `SocketException`.
                (e instanceof InterruptedIOException && !(e instanceof SocketTimeoutException))
                // see `java.nio.channels.InterruptibleChannel`
                // and `java.net.Socket.connect`, `java.net.Socket.getOutputStream`/`getInputStream`
                || e instanceof ClosedByInterruptException
                // see `java.net.Socket.connect`, `java.net.Socket.getOutputStream`/`getInputStream`
                || (e instanceof SocketException && Thread.currentThread().isInterrupted())) {
            // The interrupted status is not cleared before throwing `ClosedByInterruptException`/`SocketException`,
            // so we do not need to reinstate it.
            // `InterruptedIOException` does not specify how it behaves with regard to the interrupted status, so we do nothing.
            return Optional.of(new MongoInterruptedException(message, (Exception) e));
        } else {
            return Optional.empty();
        }
    }

    private InterruptionUtil() {
    }
}
