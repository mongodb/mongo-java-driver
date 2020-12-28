/*
 * Copyright 2008-present MongoDB, Inc.
 * Copyright 2012 The Netty Project
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

package com.mongodb.connection.netty;

/**
 * Handle to a timeout that allows a cancellation using {@link #cancel()}.
 */
public interface TimeoutHandle {
    TimeoutHandle NOOP = new NoopTimeoutHandle();

    /**
     * Cancels the timeout.
     */
    void cancel();

    /**
     * Handle to a non-existing timeout.
     */
    final class NoopTimeoutHandle implements TimeoutHandle {
        @Override
        public void cancel() {
        }
    }
}
