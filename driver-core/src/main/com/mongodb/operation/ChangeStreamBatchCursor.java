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

package com.mongodb.operation;

import com.mongodb.Function;
import com.mongodb.MongoChangeStreamException;
import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSocketException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.binding.ReadBinding;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;

import java.util.ArrayList;
import java.util.List;

final class ChangeStreamBatchCursor<T> implements BatchCursor<T> {
    private final ReadBinding binding;
    private final ChangeStreamOperation<T> changeStreamOperation;

    private BsonDocument resumeToken;
    private BatchCursor<RawBsonDocument> wrapped;

    ChangeStreamBatchCursor(final ChangeStreamOperation<T> changeStreamOperation,
                            final BatchCursor<RawBsonDocument> wrapped,
                            final ReadBinding binding) {
        this.changeStreamOperation = changeStreamOperation;
        this.resumeToken = changeStreamOperation.getResumeToken();
        this.wrapped = wrapped;
        this.binding = binding.retain();
    }

    BatchCursor<RawBsonDocument> getWrapped() {
        return wrapped;
    }

    @Override
    public boolean hasNext() {
        return resumeableOperation(new Function<BatchCursor<RawBsonDocument>, Boolean>() {
            @Override
            public Boolean apply(final BatchCursor<RawBsonDocument> queryBatchCursor) {
                return queryBatchCursor.hasNext();
            }
        });
    }

    @Override
    public List<T> next() {
        return resumeableOperation(new Function<BatchCursor<RawBsonDocument>, List<T>>() {
            @Override
            public List<T> apply(final BatchCursor<RawBsonDocument> queryBatchCursor) {
                return convertResults(queryBatchCursor.next());
            }
        });
    }

    @Override
    public List<T> tryNext() {
        return resumeableOperation(new Function<BatchCursor<RawBsonDocument>, List<T>>() {
            @Override
            public List<T> apply(final BatchCursor<RawBsonDocument> queryBatchCursor) {
                return convertResults(queryBatchCursor.tryNext());
            }
        });
    }

    @Override
    public void close() {
        wrapped.close();
        binding.release();
    }

    @Override
    public void setBatchSize(final int batchSize) {
        wrapped.setBatchSize(batchSize);
    }

    @Override
    public int getBatchSize() {
        return wrapped.getBatchSize();
    }

    @Override
    public ServerCursor getServerCursor() {
        return wrapped.getServerCursor();
    }

    @Override
    public ServerAddress getServerAddress() {
        return wrapped.getServerAddress();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not implemented!");
    }

    private List<T> convertResults(final List<RawBsonDocument> rawDocuments) {
        List<T> results = null;
        if (rawDocuments != null) {
            results = new ArrayList<T>();
            for (RawBsonDocument rawDocument : rawDocuments) {
                if (!rawDocument.containsKey("_id")) {
                    throw new MongoChangeStreamException("Cannot provide resume functionality when the resume token is missing.");
                }
                resumeToken = rawDocument.getDocument("_id");
                results.add(rawDocument.decode(changeStreamOperation.getDecoder()));
            }
        }
        return results;
    }

    <R> R resumeableOperation(final Function<BatchCursor<RawBsonDocument>, R> function) {
        while (true) {
            try {
                return function.apply(wrapped);
            } catch (MongoNotPrimaryException e) {
                // Ignore
            } catch (MongoCursorNotFoundException w) {
                // Ignore
            } catch (MongoSocketException e) {
                // Ignore
            }
            wrapped.close();
            wrapped = ((ChangeStreamBatchCursor<T>) changeStreamOperation.resumeAfter(resumeToken).execute(binding)).getWrapped();
            binding.release(); // release the new change stream batch cursor's reference to the binding
        }
    }
}
