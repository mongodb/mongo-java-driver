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

import org.mongodb.Codec;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.command.MongoCommand;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoGetMore;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoReplace;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.result.CommandResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.WriteResult;

public interface MongoAsyncOperations {
    MongoFuture<CommandResult> asyncCommand(String database, MongoCommand commandOperation, Codec<Document> codec);

    <T> MongoFuture<QueryResult<T>> asyncQuery(final MongoNamespace namespace, MongoFind find, Encoder<Document> queryEncoder,
                                               Decoder<T> resultDecoder);

    <T> MongoFuture<QueryResult<T>> asyncGetMore(final MongoNamespace namespace, MongoGetMore getMore, Decoder<T> resultDecoder);

    <T> MongoFuture<WriteResult> asyncInsert(MongoNamespace namespace, MongoInsert<T> insert, Encoder<T> encoder);

    MongoFuture<WriteResult> asyncUpdate(final MongoNamespace namespace, MongoUpdate update, Encoder<Document> queryEncoder);

    <T> MongoFuture<WriteResult> asyncReplace(MongoNamespace namespace, MongoReplace<T> replace, Encoder<Document> queryEncoder,
                                              Encoder<T> encoder);

    MongoFuture<WriteResult> asyncRemove(final MongoNamespace namespace, MongoRemove remove, Encoder<Document> queryEncoder);
}
