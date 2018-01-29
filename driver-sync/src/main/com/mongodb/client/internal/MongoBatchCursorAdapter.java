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

package com.mongodb.client.internal;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import com.mongodb.operation.BatchCursor;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 *
 * @param <T> the result type
 */
public class MongoBatchCursorAdapter<T> implements MongoCursor<T> {
    private final BatchCursor<T> batchCursor;
    private List<T> curBatch;
    private int curPos;

    public MongoBatchCursorAdapter(final BatchCursor<T> batchCursor) {
        this.batchCursor = batchCursor;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Cursors do not support removal");
    }

    @Override
    public void close() {
        batchCursor.close();
    }

    @Override
    public boolean hasNext() {
        return curBatch != null || batchCursor.hasNext();
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        if (curBatch == null) {
            curBatch = batchCursor.next();
        }

        return getNextInBatch();
    }

    @Override
    public T tryNext() {
        if (curBatch == null) {
            curBatch = batchCursor.tryNext();
        }

        return curBatch == null ? null : getNextInBatch();
    }

    @Override
    public ServerCursor getServerCursor() {
        return batchCursor.getServerCursor();
    }

    @Override
    public ServerAddress getServerAddress() {
        return batchCursor.getServerAddress();
    }

    private T getNextInBatch() {
        T nextInBatch = curBatch.get(curPos);
        if (curPos < curBatch.size() - 1) {
            curPos++;
        } else {
            curBatch = null;
            curPos = 0;
        }
        return nextInBatch;
    }
}
