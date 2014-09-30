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

import com.mongodb.CommandFailureException;
import com.mongodb.Function;
import com.mongodb.MongoNamespace;
import com.mongodb.async.MongoFuture;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ReadBinding;
import com.mongodb.connection.Connection;
import com.mongodb.protocol.QueryProtocol;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.FindOperationHelper.queryResultToList;
import static com.mongodb.operation.FindOperationHelper.queryResultToListAsync;
import static com.mongodb.operation.OperationHelper.serverIsAtLeastVersionTwoDotEight;
import static com.mongodb.operation.OperationHelper.withConnection;

/**
 * An operation that lists the indexes that have been created on a collection.  For flexibility, the type of each document returned is
 * generic.
 *
 * @param <T> the operations result type.
 * @since 3.0
 */
public class ListIndexesOperation<T> implements AsyncReadOperation<List<T>>, ReadOperation<List<T>> {
    private final MongoNamespace namespace;
    private final Decoder<T> decoder;

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param decoder   the decoder for the result documents.
     */
    public ListIndexesOperation(final MongoNamespace namespace, final Decoder<T> decoder) {
        this.namespace = notNull("namespace", namespace);
        this.decoder = notNull("decoder", decoder);
    }

    @Override
    public List<T> execute(final ReadBinding binding) {
        return withConnection(binding, new OperationHelper.CallableWithConnection<List<T>>() {
            @Override
            public List<T> call(final Connection connection) {
                if (serverIsAtLeastVersionTwoDotEight(connection)) {
                    try {
                        return executeWrappedCommandProtocol(namespace.getDatabaseName(), getCommand(), new BsonDocumentCodec(), binding,
                                                             transformer());
                    } catch (CommandFailureException e) {
                        return CommandOperationHelper.rethrowIfNotNamespaceError(e, new ArrayList<T>());
                    }
                } else {
                    return queryResultToList(getIndexNamespace(), getProtocol(), decoder, binding);
                }
            }
        });

    }

    @Override
    public MongoFuture<List<T>> executeAsync(final AsyncReadBinding binding) {
        return withConnection(binding, new OperationHelper.AsyncCallableWithConnection<List<T>>() {
            @Override
            public MongoFuture<List<T>> call(final Connection connection) {
                if (serverIsAtLeastVersionTwoDotEight(connection)) {
                    return CommandOperationHelper.rethrowIfNotNamespaceError(executeWrappedCommandProtocolAsync(namespace.getDatabaseName(),
                                                                                                                getCommand(),
                                                                                                                connection,
                                                                                                                transformer()),
                                                                             new ArrayList<T>());
                } else {
                    return queryResultToListAsync(getIndexNamespace(), getProtocol(), decoder, binding);
                }
            }
        });
    }

    private BsonDocument asQueryDocument() {
        return new BsonDocument("ns", new BsonString(namespace.getFullName()));
    }

    private MongoNamespace getIndexNamespace() {
        return new MongoNamespace(namespace.getDatabaseName(), "system.indexes");
    }

    private QueryProtocol<T> getProtocol() {
        return new QueryProtocol<T>(getIndexNamespace(), 0, 0, asQueryDocument(), null, decoder);
    }

    private BsonDocument getCommand() {
        return new BsonDocument("listIndexes", new BsonString(namespace.getCollectionName()));
    }

    private Function<BsonDocument, List<T>> transformer() {
        return new Function<BsonDocument, List<T>>() {
            @Override
            public List<T> apply(final BsonDocument results) {
                BsonArray bsonIndexes = results.getArray("indexes");
                List<T> indexes = new ArrayList<T>(bsonIndexes.size());
                for (BsonValue rawIndex : bsonIndexes) {
                    indexes.add(decoder.decode(new BsonDocumentReader((BsonDocument) rawIndex), DecoderContext.builder().build()));
                }
                return indexes;
            }
        };
    }

}
