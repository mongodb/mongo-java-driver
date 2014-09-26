/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.MongoNamespace;
import com.mongodb.async.MongoFuture;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ReadBinding;
import com.mongodb.protocol.QueryProtocol;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.codecs.Decoder;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.FindOperationHelper.queryResultToList;
import static com.mongodb.operation.FindOperationHelper.queryResultToListAsync;

/**
 * An operation that gets the indexes that have been created on a collection.  For flexibility,
 * the type of each document returned is generic.
 *
 * @param <T> the operations result type.
 *
 * @since 3.0
 */
public class GetIndexesOperation<T> implements AsyncReadOperation<List<T>>, ReadOperation<List<T>> {
    private final MongoNamespace namespace;
    private final Decoder<T> decoder;

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param decoder the decoder for the result documents.
     */
    public GetIndexesOperation(final MongoNamespace namespace, final Decoder<T> decoder) {
        this.namespace = notNull("namespace", namespace);
        this.decoder = notNull("decoder", decoder);
    }

    @Override
    public List<T> execute(final ReadBinding binding) {
        return queryResultToList(getIndexNamespace(), getProtocol(), decoder, binding);
    }

    @Override
    public MongoFuture<List<T>> executeAsync(final AsyncReadBinding binding) {
        return queryResultToListAsync(getIndexNamespace(), getProtocol(), decoder, binding);
    }

    private BsonDocument asQueryDocument() {
        return new BsonDocument("ns", new BsonString(namespace.getFullName()));
    }

    private MongoNamespace getIndexNamespace() {
        return new MongoNamespace(namespace.getDatabaseName(), "system.indexes");
    }

    private QueryProtocol<T> getProtocol() {
        return new QueryProtocol<T>(getIndexNamespace(), 0, 0, 0, asQueryDocument(), null, decoder);
    }

}
