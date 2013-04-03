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
 *
 */

package org.mongodb.async;

import org.mongodb.Document;
import org.mongodb.MongoNamespace;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.MongoCommand;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoReplace;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.result.CommandResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.WriteResult;
import org.mongodb.Decoder;
import org.mongodb.Encoder;
import org.mongodb.Codec;

import java.util.concurrent.Future;

public interface MongoAsyncConnector {
    Future<CommandResult> asyncCommand(String database, MongoCommand commandOperation, Codec<Document> codec);

    void asyncCommand(String database, MongoCommand commandOperation, Codec<Document> codec,
                      SingleResultCallback<CommandResult> callback);

    <T> Future<QueryResult<T>> asyncQuery(final MongoNamespace namespace, MongoFind find, Encoder<Document> queryEncoder,
                                          Decoder<T> resultDecoder);

    <T> void asyncQuery(final MongoNamespace namespace, MongoFind find, Encoder<Document> queryEncoder,
                        Decoder<T> resultDecoder, SingleResultCallback<QueryResult<T>> callback);

    <T> Future<QueryResult<T>> asyncGetMore(final MongoNamespace namespace, GetMore getMore, Decoder<T> resultDecoder);

    <T> void asyncGetMore(final MongoNamespace namespace, GetMore getMore, Decoder<T> resultDecoder,
                          SingleResultCallback<QueryResult<T>> callback);

    <T> Future<WriteResult> asyncInsert(MongoNamespace namespace, MongoInsert<T> insert, Encoder<T> encoder);

    <T> void asyncInsert(MongoNamespace namespace, MongoInsert<T> insert, Encoder<T> encoder,
                         SingleResultCallback<WriteResult> callback);

    Future<WriteResult> asyncUpdate(final MongoNamespace namespace, MongoUpdate update, Encoder<Document> queryEncoder);

    void asyncUpdate(final MongoNamespace namespace, MongoUpdate update, Encoder<Document> queryEncoder,
                     SingleResultCallback<WriteResult> callback);

    <T> Future<WriteResult> asyncReplace(MongoNamespace namespace, MongoReplace<T> replace, Encoder<Document> queryEncoder,
                                         Encoder<T> encoder);

    <T> void asyncReplace(MongoNamespace namespace, MongoReplace<T> replace, Encoder<Document> queryEncoder,
                          Encoder<T> encoder, SingleResultCallback<WriteResult> callback);

    Future<WriteResult> asyncRemove(final MongoNamespace namespace, MongoRemove remove, Encoder<Document> queryEncoder);

    void asyncRemove(final MongoNamespace namespace, MongoRemove remove, Encoder<Document> queryEncoder,
                     SingleResultCallback<WriteResult> callback);
}
