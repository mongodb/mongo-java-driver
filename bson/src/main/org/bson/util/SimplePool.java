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

// SimplePool.java

package org.bson.util;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

// TODO: either make this class safe or replace it.
// TODO: it's also not efficient in the way it manages memory
// TODO: it should keep a list of what's been removed from the pool and only add things that have been removed
public abstract class SimplePool<T> {
    private final int max;
    private final Queue<T> stored = new ConcurrentLinkedQueue<T>();

    public SimplePool(final int max) {
        this.max = max;
    }

    public SimplePool() {
        this(1000);
    }

    protected abstract T createNew();

    protected boolean ok(final T t) {
        return true;
    }

    public T get() {
        final T t = stored.poll();
        if (t != null) {
            return t;
        }
        return createNew();
    }

    public void done(final T t) {
        if (!ok(t)) {
            return;
        }

        if (stored.size() > max) {
            return;
        }
        stored.add(t);
    }
}
