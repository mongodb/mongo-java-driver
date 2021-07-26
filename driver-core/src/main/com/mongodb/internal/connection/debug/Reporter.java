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
package com.mongodb.internal.connection.debug;

import com.mongodb.annotations.ThreadSafe;
import com.mongodb.lang.Nullable;

@ThreadSafe
interface Reporter {
    /**
     * This method is meant to be used in both synchronous and asynchronous code.
     * <ul>
     *     <li>Synchronous mode, {@code callback} is {@code null}</li>
     *     <ul>
     *         <li>If {@link Debugger.ReportingMode#LOG_AND_THROW}, then completes abruptly;
     *         otherwise returns {@code false}.</li>
     *         <li>Callers do not need to handle the return value.</li>
     *     </ul>
     *     <li>Asynchronous mode, {@code callback} is not {@code null}</li>
     *     <ul>
     *         <li>If {@link Debugger.ReportingMode#LOG_AND_THROW},
     *         then executes the {@link FailureCallback} and returns {@code true} unless the callback completes abruptly;
     *         otherwise returns {@code false}.</li>
     *         <li>If the method returns {@code true}, then the calling method that contains the driver logic
     *         (as opposed to containing the debugging logic) must stop executing the driver logic and return;
     *         note that such method may not be the immediate caller.
     *         Following this rule allows to call {@link #report(MongoDebuggingException, FailureCallback)} in asynchronous code
     *         without violating the rule that a callback must be executed and must not be executed more than once.</li>
     *     </ul>
     * </ul>
     *
     * @throws MongoDebuggingException If completes abruptly in synchronous mode.
     */
    /* Alternatively to returning a value and burdening the caller with handling it, the method could have accepted one more callback
     * of type `SuccessCallback`. While this approach could result in writing prettier/simpler debugging code, it would increase the effect
     * from debugger presence on stack traces in happy paths. I think reducing this effect is more important than making the debugging
     * code prettier. */
    boolean report(MongoDebuggingException e, @Nullable FailureCallback callback) throws MongoDebuggingException;

    interface FailureCallback {
        void execute(RuntimeException e);
    }
}
