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

import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.internal.async.AsyncRunnable.beginAsync;

final class CursorHelper {

    static BsonDocument getCursorDocumentFromBatchSize(@Nullable final Integer batchSize) {
        return batchSize == null ? new BsonDocument() : new BsonDocument("batchSize", new BsonInt32(batchSize));
    }

    public static <T> void exhaustCursorAsync(final AsyncBatchCursor<T> cursor, final SingleResultCallback<List<List<T>>> finalCallback) {
        List<List<T>> results = new ArrayList<>();

        beginAsync().thenRunDoWhileLoop(iterationCallback -> {
                    beginAsync().
                            thenSupply(cursor::next)
                            .thenConsume((batch, callback) -> {
                                if (batch != null && !batch.isEmpty()) {
                                    results.add(batch);
                                }
                                callback.complete(callback);
                            }).finish(iterationCallback);
                }, () -> !cursor.isClosed())
                .<List<List<T>>>thenSupply(callback -> {
                    callback.complete(results);
                }).finish(finalCallback);
    }

    private CursorHelper() {
    }
}
