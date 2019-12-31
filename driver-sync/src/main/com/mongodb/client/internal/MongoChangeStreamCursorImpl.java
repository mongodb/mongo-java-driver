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
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.internal.operation.AggregateResponseBatchCursor;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.codecs.Decoder;

import java.util.List;
import java.util.NoSuchElementException;

public class MongoChangeStreamCursorImpl<T> implements MongoChangeStreamCursor<T> {
    private final AggregateResponseBatchCursor<RawBsonDocument> batchCursor;
    private final Decoder<T> decoder;
    private List<RawBsonDocument> curBatch;
    private int curPos;
    private BsonDocument resumeToken;

    public MongoChangeStreamCursorImpl(final BatchCursor<RawBsonDocument> batchCursor, final Decoder<T> decoder,
                                       @Nullable final BsonDocument initialResumeToken) {
        this.batchCursor = (AggregateResponseBatchCursor<RawBsonDocument>) batchCursor;
        this.decoder = decoder;
        this.resumeToken = initialResumeToken;
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

    @Nullable
    @Override
    public T tryNext() {
        if (curBatch == null) {
            curBatch = batchCursor.tryNext();
        }

        if (curBatch == null) {
            if (batchCursor.getPostBatchResumeToken() != null) {
                resumeToken = batchCursor.getPostBatchResumeToken();
            }
        }

        return curBatch == null ? null : getNextInBatch();
    }

    @Nullable
    @Override
    public ServerCursor getServerCursor() {
        return batchCursor.getServerCursor();
    }

    @Override
    public ServerAddress getServerAddress() {
        return batchCursor.getServerAddress();
    }

    private T getNextInBatch() {
        RawBsonDocument nextInBatch = curBatch.get(curPos);
        resumeToken = nextInBatch.getDocument("_id");
        if (curPos < curBatch.size() - 1) {
            curPos++;
        } else {
            curBatch = null;
            curPos = 0;
            if (batchCursor.getPostBatchResumeToken() != null) {
                resumeToken = batchCursor.getPostBatchResumeToken();
            }
        }

        return nextInBatch.decode(decoder);
    }

    @Nullable
    public BsonDocument getResumeToken() {
        return resumeToken;
    }
}
