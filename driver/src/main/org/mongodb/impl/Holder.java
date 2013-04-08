/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.impl;

import org.mongodb.MongoClosedException;
import org.mongodb.MongoInterruptedException;
import org.mongodb.MongoTimeoutException;
import org.mongodb.annotations.ThreadSafe;

// Simple abstraction over a volatile Object reference that starts as null.  The get method blocks until held
// is not null. The set method notifies all, thus waking up all getters.
@ThreadSafe
class Holder {
    private volatile Object held;
    private volatile boolean closed;
    private final int timeout;

    Holder(final int timeout) {
        this.timeout = timeout;
    }

    // blocks until replica set is set, or a timeout occurs
    synchronized Object get() {
        if (held == null) {
            try {
                wait(timeout);
            } catch (InterruptedException e) {
                throw new MongoInterruptedException("Interrupted while waiting for next update to replica set status", e);
            }
        }
        if (closed) {
            throw new MongoClosedException("Already closed");
        }

        if (held == null) {
            throw new MongoTimeoutException("Timeout waiting for response from replica set or sharded cluster");
        }
        return held;
    }

    // set the replica set to a non-null value and notifies all threads waiting.
    synchronized void set(final Object newHeld) {
        if (newHeld == null) {
            throw new IllegalArgumentException("held can not be null");
        }

        this.held = newHeld;
        notifyAll();
    }

    // blocks until the replica set is set again
//    synchronized void waitForNextUpdate() {  // TODO: currently unused
//        try {
//            wait(timeout);
//        } catch (InterruptedException e) {
//            throw new MongoInterruptedException("Interrupted while waiting for next update to replica set status", e);
//        }
//    }

    synchronized void close() {
        held = null;
        closed = true;
        notifyAll();
    }

    boolean isClosed() {
        return closed;
    }

    @Override
    public String toString() {
        Object cur = this.held;
        if (cur != null) {
            return cur.toString();
        }
        return "none";
    }
}
