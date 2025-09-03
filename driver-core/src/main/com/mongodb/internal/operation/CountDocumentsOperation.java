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

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class CountDocumentsOperation implements ReadOperationSimple<Long> {
    private static final String COMMAND_NAME = "aggregate";
    private static final Decoder<BsonDocument> DECODER = new BsonDocumentCodec();
    private final MongoNamespace namespace;
    private boolean retryReads;
    private BsonDocument filter;
    private BsonValue hint;
    private BsonValue comment;
    private long skip;
    private long limit;
    private Collation collation;

    public CountDocumentsOperation(final MongoNamespace namespace) {
        this.namespace = notNull("namespace", namespace);
    }

    @Nullable
    public BsonDocument getFilter() {
        return filter;
    }

    public CountDocumentsOperation filter(@Nullable final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    public CountDocumentsOperation retryReads(final boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    public boolean getRetryReads() {
        return retryReads;
    }

    @Nullable
    public BsonValue getHint() {
        return hint;
    }

    public CountDocumentsOperation hint(@Nullable final BsonValue hint) {
        this.hint = hint;
        return this;
    }

    public long getLimit() {
        return limit;
    }

    public CountDocumentsOperation limit(final long limit) {
        this.limit = limit;
        return this;
    }

    public long getSkip() {
        return skip;
    }

    public CountDocumentsOperation skip(final long skip) {
        this.skip = skip;
        return this;
    }

    @Nullable
    public Collation getCollation() {
        return collation;
    }

    public CountDocumentsOperation collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Nullable
    public BsonValue getComment() {
        return comment;
    }

    public CountDocumentsOperation comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }

    @Override
    public Long execute(final ReadBinding binding) {
        try (BatchCursor<BsonDocument> cursor = getAggregateOperation().execute(binding)) {
            return cursor.hasNext() ? getCountFromAggregateResults(cursor.next()) : 0;
        }
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<Long> callback) {
        getAggregateOperation().executeAsync(binding, (result, t) -> {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                result.next((result1, t1) -> {
                    if (t1 != null) {
                        callback.onResult(null, t1);
                    } else {
                        callback.onResult(getCountFromAggregateResults(result1), null);
                    }
                });
            }
        });
    }

    private AggregateOperation<BsonDocument> getAggregateOperation() {
        return new AggregateOperation<>(namespace, getPipeline(), DECODER)
                .retryReads(retryReads)
                .collation(collation)
                .comment(comment)
                .hint(hint);
    }

    private List<BsonDocument> getPipeline() {
        ArrayList<BsonDocument> pipeline = new ArrayList<>();
        pipeline.add(new BsonDocument("$match", filter != null ? filter : new BsonDocument()));
        if (skip > 0) {
            pipeline.add(new BsonDocument("$skip", new BsonInt64(skip)));
        }
        if (limit > 0) {
            pipeline.add(new BsonDocument("$limit", new BsonInt64(limit)));
        }
        pipeline.add(new BsonDocument("$group", new BsonDocument("_id", new BsonInt32(1))
                .append("n", new BsonDocument("$sum", new BsonInt32(1)))));
        return pipeline;
    }

    private Long getCountFromAggregateResults(@Nullable final List<BsonDocument> results) {
        if (results == null || results.isEmpty()) {
            return 0L;
        } else {
            return results.get(0).getNumber("n").longValue();
        }
    }
}
