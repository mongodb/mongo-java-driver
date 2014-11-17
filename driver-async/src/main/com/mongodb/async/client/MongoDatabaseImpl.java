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

package com.mongodb.async.client;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.async.MongoFuture;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.options.OperationOptions;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.CommandReadOperation;
import com.mongodb.operation.CommandWriteOperation;
import com.mongodb.operation.CreateCollectionOperation;
import com.mongodb.operation.DropDatabaseOperation;
import com.mongodb.operation.ListCollectionNamesOperation;
import org.bson.Document;

import java.util.List;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static org.bson.BsonDocumentWrapper.asBsonDocument;

class MongoDatabaseImpl implements MongoDatabase {
    private final OperationOptions options;
    private final String name;
    private final AsyncOperationExecutor executor;

    MongoDatabaseImpl(final String name, final OperationOptions options, final AsyncOperationExecutor executor) {
        this.name = name;
        this.options = options;
        this.executor = executor;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public MongoCollection<Document> getCollection(final String collectionName) {
        return getCollection(collectionName, OperationOptions.builder().build());
    }

    @Override
    public MongoCollection<Document> getCollection(final String collectionName, final OperationOptions options) {
        return getCollection(collectionName, Document.class, options);
    }

    @Override
    public <T> MongoCollection<T> getCollection(final String collectionName, final Class<T> clazz) {
        return getCollection(collectionName, clazz, OperationOptions.builder().build());
    }

    @Override
    public <T> MongoCollection<T> getCollection(final String collectionName, final Class<T> clazz,
                                                final OperationOptions options) {
        return new MongoCollectionImpl<T>(new MongoNamespace(name, collectionName), clazz, options.withDefaults(this.options),
                                          executor);
    }

    @Override
    public MongoFuture<Void> dropDatabase() {
        return executor.execute(new DropDatabaseOperation(name));
    }

    @Override
    public MongoFuture<List<String>> getCollectionNames() {
        return executor.execute(new ListCollectionNamesOperation(name), primary());
    }

    @Override
    public MongoFuture<Void> createCollection(final String collectionName) {
        return createCollection(collectionName, new CreateCollectionOptions());
    }

    @Override
    public MongoFuture<Void> createCollection(final String collectionName, final CreateCollectionOptions options) {
        return executor.execute(new CreateCollectionOperation(name, collectionName)
                                .capped(options.isCapped())
                                .sizeInBytes(options.getSizeInBytes())
                                .autoIndex(options.isAutoIndex())
                                .maxDocuments(options.getMaxDocuments())
                                .usePowerOf2Sizes(options.isUsePowerOf2Sizes()));
    }

    @Override
    public MongoFuture<Document> executeCommand(final Object command) {
        return executor.execute(new CommandWriteOperation<Document>(getName(),
                                                                    asBsonDocument(command, options.getCodecRegistry()),
                                                                    options.getCodecRegistry().get(Document.class)));
    }

    @Override
    public MongoFuture<Document> executeCommand(final Object command, final ReadPreference readPreference) {
        notNull("readPreference", readPreference);
        return executor.execute(new CommandReadOperation<Document>(getName(),
                                                                   asBsonDocument(command, options.getCodecRegistry()),
                                                                   options.getCodecRegistry().get(Document.class)),
                                readPreference);
    }

    @Override
    public OperationOptions getOptions() {
        return options;
    }

}
