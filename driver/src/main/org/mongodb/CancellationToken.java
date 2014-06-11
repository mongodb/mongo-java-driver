/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package org.mongodb;

public class CancellationToken {

    /**
     * Can this Token be cancelled
     */
    private final boolean canBeCanceled;

    /**
     * Has cancellation has been requested.
     */
    private volatile boolean cancelled;

    /**
     * Create a cancellable CancellationToken
     */
    public CancellationToken() {
        this(true);
    }

    /**
     * Create a CancellationToken
     * @param canBeCanceled indicates if it can be cancelled
     */
    public CancellationToken(final boolean canBeCanceled) {
        this.canBeCanceled = canBeCanceled;
        this.cancelled = false;
    }

    public static CancellationToken notCancellable() {
        return new CancellationToken(false);
    }

    /**
     * @return the cancelled flag
     */
    public boolean cancellationRequested() {
        return cancelled;
    }

    /**
     * Request cancellation
     */
    public void cancel() {
        if (canBeCanceled) {
            cancelled = true;
        }
    }

}
