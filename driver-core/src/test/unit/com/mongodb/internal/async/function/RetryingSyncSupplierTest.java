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
package com.mongodb.internal.async.function;

import com.mongodb.internal.async.function.RetryPolicy.Decision.RetryAttemptInfo;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

final class RetryingSyncSupplierTest {
    @Test
    void doWhileDisabledThrowsAtFirstAttempt() {
        RetryControl<?> retryControl = new RetryControl<>(new AssertingUnusedRetryPolicy(false));
        RuntimeException exception = new RuntimeException();
        RetryingSyncSupplier<Void> retryingSupplier = new RetryingSyncSupplier<>(
                retryControl,
                () -> {
                    retryControl.doWhileDisabled(() -> {
                        throw exception;
                    });
                    return null;
                });
        assertSame(exception, assertThrows(exception.getClass(), () -> retryingSupplier.get()));
        assertTrue(retryControl.isFirstAttempt());
    }

    @Test
    void doWhileDisabledThrowsAtSecondAttempt() {
        RetryControl<?> retryControl = new RetryControl<>(new AssertingUnusedRetryPolicy(true));
        RuntimeException exception = new RuntimeException();
        RetryingSyncSupplier<Void> retryingSupplier = new RetryingSyncSupplier<>(
                retryControl,
                () -> {
                    if (retryControl.isFirstAttempt()) {
                        throw new RuntimeException();
                    }
                    retryControl.doWhileDisabled(() -> {
                        throw exception;
                    });
                    return null;
                });
        assertSame(exception, assertThrows(exception.getClass(), () -> retryingSupplier.get()));
        assertEquals(1, retryControl.attempt());
    }

    @Test
    void doWhileDisabledCompletesNormally() {
        RetryControl<?> retryControl = new RetryControl<>(new AssertingUnusedRetryPolicy(true));
        Object result = new Object();
        RetryingSyncSupplier<Object> retryingSupplier = new RetryingSyncSupplier<>(
                retryControl,
                () -> {
                    Object doWhileDisabledResult = retryControl.doWhileDisabled(() -> result);
                    if (retryControl.isFirstAttempt()) {
                        throw new RuntimeException();
                    }
                    return doWhileDisabledResult;
                });
        assertSame(result, retryingSupplier.get());
        assertEquals(1, retryControl.attempt());
    }

    @Test
    void doWhileDisabledNestedThrowsAtFirstAttempt() {
        RetryControl<?> retryControl = new RetryControl<>(new AssertingUnusedRetryPolicy(false));
        RuntimeException exception = new RuntimeException();
        RetryingSyncSupplier<Void> retryingSupplier = new RetryingSyncSupplier<>(
                retryControl,
                () -> {
                    retryControl.doWhileDisabled(() -> {
                        retryControl.doWhileDisabled(() -> {
                            throw exception;
                        });
                        return null;
                    });
                    return null;
                });
        assertSame(exception, assertThrows(exception.getClass(), () -> retryingSupplier.get()));
        assertTrue(retryControl.isFirstAttempt());
    }

    static final class AssertingUnusedRetryPolicy implements RetryPolicy {
        private final boolean skipFailingOnFirstAttempt;

        AssertingUnusedRetryPolicy(final boolean skipFailingOnFirstAttempt) {
            this.skipFailingOnFirstAttempt = skipFailingOnFirstAttempt;
        }

        @Override
        public Decision onAttemptFailure(final RetryContext retryContext, final Throwable attemptFailedResult) {
            if (skipFailingOnFirstAttempt && retryContext.isFirstAttempt()) {
                return new Decision(attemptFailedResult, new RetryAttemptInfo());
            }
            return fail();
        }
    }
}
