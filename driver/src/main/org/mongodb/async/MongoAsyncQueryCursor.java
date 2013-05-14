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

package org.mongodb.async;

import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoServerBinding;
import org.mongodb.impl.SingleConnectionMongoServerBinding;
import org.mongodb.io.BufferPool;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoGetMore;
import org.mongodb.operation.QueryOption;
import org.mongodb.result.QueryResult;
import org.mongodb.result.ServerCursor;

import java.nio.ByteBuffer;

// TODO: kill cursor on early breakout
public class MongoAsyncQueryCursor<T> {
    private final MongoNamespace namespace;
    private final MongoFind find;
    private Encoder<Document> queryEncoder;
    private final Decoder<T> decoder;
    private final BufferPool<ByteBuffer> bufferPool;
    private final AsyncBlock<? super T> block;
    private final MongoServerBinding binding;
    private long numFetchedSoFar;
    private ServerCursor cursor;

    public MongoAsyncQueryCursor(final MongoNamespace namespace, final MongoFind find, final Encoder<Document> queryEncoder,
                                 final Decoder<T> decoder, final MongoServerBinding defaultBinding, final BufferPool<ByteBuffer> bufferPool,
                                 final AsyncBlock<? super T> block) {
        this.namespace = namespace;
        this.find = find;
        this.queryEncoder = queryEncoder;
        this.decoder = decoder;
        this.bufferPool = bufferPool;
        this.block = block;
        if (find.getOptions().contains(QueryOption.Exhaust)) {
            this.binding = new SingleConnectionMongoServerBinding(defaultBinding);
        }
        else {
            this.binding = defaultBinding;
        }
    }

    public void start() {
        new AsyncQueryOperation<T>(namespace, find, queryEncoder, decoder, bufferPool).execute(binding)
                .register(new QueryResultSingleResultCallback());

    }

    private class QueryResultSingleResultCallback implements SingleResultCallback<QueryResult<T>> {
        @Override
        public void onResult(final QueryResult<T> result, final MongoException e) {
            if (e != null) {
                handleExhaustCleanup();
                block.done();  // TODO: Error handling.  Call done with an ExecutionException.
            }
            else {
                cursor = result.getCursor();
            }

            boolean breakEarly = false;

            for (final T cur : result.getResults()) {
                numFetchedSoFar++;
                if (find.getLimit() > 0 && numFetchedSoFar > find.getLimit()) {
                    breakEarly = true;
                    break;
                }

                if (!block.run(cur)) {
                    breakEarly = true;
                    break;
                }
            }

            if (result.getCursor() == null || breakEarly) {
                handleExhaustCleanup();
                block.done();
            }
            else {
                // get more results
                AsyncGetMoreOperation<T> getMoreOperation = new AsyncGetMoreOperation<T>(namespace,
                        new MongoGetMore(result.getCursor(), find.getLimit(), find.getBatchSize(), numFetchedSoFar), decoder,
                        bufferPool);
                if (find.getOptions().contains(QueryOption.Exhaust)) {
                    getMoreOperation.executeReceive(binding).register(this);
                }
                else {
                    getMoreOperation.execute(binding).register(this);
                }
            }
        }

        private void handleExhaustCleanup() {
            if (find.getOptions().contains(QueryOption.Exhaust)) {
                MongoFuture<Void> future =  new AsyncGetMoreOperation<T>(namespace, new MongoGetMore(cursor, find.getLimit(),
                        find.getBatchSize(),
                        numFetchedSoFar), null, bufferPool).executeDiscard(binding);
                future.register(new SingleResultCallback<Void>() {
                    @Override
                    public void onResult(final Void result, final MongoException e) {
                        binding.close();
                    }
                });
            }
        }
    }

}
