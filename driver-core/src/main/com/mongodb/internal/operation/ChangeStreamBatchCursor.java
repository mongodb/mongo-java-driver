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

package com.mongodb.internal.operation;

import java.util.function.Function;
import com.mongodb.MongoChangeStreamException;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.operation.OperationHelper.CallableWithSource;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.bson.codecs.Decoder;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static com.mongodb.internal.operation.ChangeStreamBatchCursorHelper.isRetryableError;
import static com.mongodb.internal.operation.OperationHelper.withReadConnectionSource;

final class ChangeStreamBatchCursor<T> implements AggregateResponseBatchCursor<T> {
    private final ReadBinding binding;
    private final ChangeStreamOperation<T> changeStreamOperation;
    private final int maxWireVersion;

    private AggregateResponseBatchCursor<RawBsonDocument> wrapped;
    private BsonDocument resumeToken;
    private volatile boolean closed;

    ChangeStreamBatchCursor(final ChangeStreamOperation<T> changeStreamOperation,
                            final AggregateResponseBatchCursor<RawBsonDocument> wrapped,
                            final ReadBinding binding,
                            final BsonDocument resumeToken,
                            final int maxWireVersion) {
        this.changeStreamOperation = changeStreamOperation;
        this.binding = binding.retain();
        this.wrapped = wrapped;
        this.resumeToken = resumeToken;
        this.maxWireVersion = maxWireVersion;
    }

    AggregateResponseBatchCursor<RawBsonDocument> getWrapped() {
        return wrapped;
    }

    @Override
    public boolean hasNext() {
        return resumeableOperation(new Function<AggregateResponseBatchCursor<RawBsonDocument>, Boolean>() {
            @Override
            public Boolean apply(final AggregateResponseBatchCursor<RawBsonDocument> queryBatchCursor) {
                try {
                    return queryBatchCursor.hasNext();
                } finally {
                    cachePostBatchResumeToken(queryBatchCursor);
                }
            }
        });
    }

    @Override
    public List<T> next() {
        return resumeableOperation(new Function<AggregateResponseBatchCursor<RawBsonDocument>, List<T>>() {
            @Override
            public List<T> apply(final AggregateResponseBatchCursor<RawBsonDocument> queryBatchCursor) {
                try {
                    return convertAndProduceLastId(queryBatchCursor.next(), changeStreamOperation.getDecoder(),
                            lastId -> resumeToken = lastId);
                } finally {
                    cachePostBatchResumeToken(queryBatchCursor);
                }
            }
        });
    }

    @Override
    public List<T> tryNext() {
        return resumeableOperation(new Function<AggregateResponseBatchCursor<RawBsonDocument>, List<T>>() {
            @Override
            public List<T> apply(final AggregateResponseBatchCursor<RawBsonDocument> queryBatchCursor) {
                try {
                    return convertAndProduceLastId(queryBatchCursor.tryNext(), changeStreamOperation.getDecoder(),
                            lastId -> resumeToken = lastId);
                } finally {
                    cachePostBatchResumeToken(queryBatchCursor);
                }
            }
        });
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            wrapped.close();
            binding.release();
        }
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

    @Override
    public BsonDocument getPostBatchResumeToken() {
        return wrapped.getPostBatchResumeToken();
    }

    @Override
    public BsonTimestamp getOperationTime() {
        return changeStreamOperation.getStartAtOperationTime();
    }

    @Override
    public boolean isFirstBatchEmpty() {
        return wrapped.isFirstBatchEmpty();
    }

    @Override
    public int getMaxWireVersion() {
        return maxWireVersion;
    }

    private void cachePostBatchResumeToken(final AggregateResponseBatchCursor<RawBsonDocument> queryBatchCursor) {
        if (queryBatchCursor.getPostBatchResumeToken() != null) {
            resumeToken = queryBatchCursor.getPostBatchResumeToken();
        }
    }

    /**
     * @param lastIdConsumer Is {@linkplain Consumer#accept(Object) called} iff {@code rawDocuments} is successfully converted
     *                       and the returned {@link List} is neither {@code null} nor {@linkplain List#isEmpty() empty}.
     */
    @Nullable
    static <T> List<T> convertAndProduceLastId(@Nullable final List<RawBsonDocument> rawDocuments,
                                               final Decoder<T> decoder,
                                               final Consumer<BsonDocument> lastIdConsumer) {
        List<T> results = null;
        if (rawDocuments != null) {
            results = new ArrayList<T>();
            for (RawBsonDocument rawDocument : rawDocuments) {
                if (!rawDocument.containsKey("_id")) {
                    throw new MongoChangeStreamException("Cannot provide resume functionality when the resume token is missing.");
                }
                results.add(rawDocument.decode(decoder));
            }
            lastIdConsumer.accept(rawDocuments.get(rawDocuments.size() - 1).getDocument("_id"));
        }
        return results;
    }

    <R> R resumeableOperation(final Function<AggregateResponseBatchCursor<RawBsonDocument>, R> function) {
        while (true) {
            try {
                return function.apply(wrapped);
            } catch (Throwable t) {
                if (!isRetryableError(t, maxWireVersion)) {
                    throw MongoException.fromThrowableNonNull(t);
                }
            }
            wrapped.close();

            withReadConnectionSource(binding, new CallableWithSource<Void>() {
                @Override
                public Void call(final ConnectionSource source) {
                    changeStreamOperation.setChangeStreamOptionsForResume(resumeToken, source.getServerDescription().getMaxWireVersion());
                    return null;
                }
            });
            wrapped = ((ChangeStreamBatchCursor<T>) changeStreamOperation.execute(binding)).getWrapped();
            binding.release(); // release the new change stream batch cursor's reference to the binding
        }
    }
}
