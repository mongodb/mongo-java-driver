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

/**
 * A token that can be canceled
 *
 * Used alongside an asynchronous loop so that the loop can be stopped.
 */
public class CancellationToken {

    /**
     * Has cancellation has been requested.
     */
    private volatile boolean cancelled;

    /**
     * Create a CancellationToken
     */
    public CancellationToken() {
        this.cancelled = false;
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
        cancelled = true;
    }

}
