package com.mongodb;

import com.mongodb.operation.BatchCursor;

import java.util.List;
import java.util.NoSuchElementException;

class MongoBatchCursorAdapter<T> implements MongoCursor<T> {
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
        return !needsNewBatch() || batchCursor.hasNext();
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        if (needsNewBatch()) {
            curBatch = batchCursor.next();
            curPos = 0;
        }

        return curBatch.get(curPos++);
    }

    @Override
    public T tryNext() {
        if (needsNewBatch()) {
            curBatch = batchCursor.tryNext();
            curPos = 0;
        }

        return curBatch == null ? null : curBatch.get(curPos++);
    }

    @Override
    public ServerCursor getServerCursor() {
        return batchCursor.getServerCursor();
    }

    @Override
    public ServerAddress getServerAddress() {
        return batchCursor.getServerAddress();
    }

    private boolean needsNewBatch() {
        return curBatch == null || curPos == curBatch.size();
    }
}
