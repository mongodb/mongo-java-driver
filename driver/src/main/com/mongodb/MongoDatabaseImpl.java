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

package com.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.options.OperationOptions;
import com.mongodb.operation.CommandReadOperation;
import com.mongodb.operation.CommandWriteOperation;
import com.mongodb.operation.CreateCollectionOperation;
import com.mongodb.operation.DropDatabaseOperation;
import com.mongodb.operation.ListCollectionsOperation;
import com.mongodb.operation.OperationExecutor;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;

class MongoDatabaseImpl implements MongoDatabase {
    private final OperationOptions options;
    private final String name;
    private final OperationExecutor executor;

    MongoDatabaseImpl(final String name, final OperationOptions options, final OperationExecutor executor) {
        this.name = name;
        this.executor = executor;
        this.options = options;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public OperationOptions getOptions() {
        return options;
    }

    @Override
    public MongoCollection<Document> getCollection(final String collectionName) {
        return getCollection(collectionName, options);
    }

    @Override
    public MongoCollection<Document> getCollection(final String collectionName, final OperationOptions operationOptions) {
        return getCollection(collectionName, Document.class, operationOptions);
    }

    @Override
    public <T> MongoCollection<T> getCollection(final String collectionName, final Class<T> clazz) {
        return getCollection(collectionName, clazz, OperationOptions.builder().build());
    }

    @Override
    public <T> MongoCollection<T> getCollection(final String collectionName, final Class<T> clazz,
                                                final OperationOptions operationOptions) {
        return new MongoCollectionImpl<T>(new MongoNamespace(name, collectionName), clazz, operationOptions.withDefaults(options),
                                          executor);
    }

    @Override
    public void dropDatabase() {
        executor.execute(new DropDatabaseOperation(name));
    }

    @Override
    public List<String> getCollectionNames() {
        return new OperationIterable<Document>(new ListCollectionsOperation<Document>(name, new DocumentCodec()), primary(), executor)
               .map(new Function<Document, String>() {
                   @Override
                   public String apply(final Document result) {
                       return (String) result.get("name");
                   }
               }).into(new ArrayList<String>());
    }

    @Override
    public void createCollection(final String collectionName) {
        createCollection(collectionName, new CreateCollectionOptions());
    }

    @Override
    public void createCollection(final String collectionName, final CreateCollectionOptions createCollectionOptions) {
        executor.execute(new CreateCollectionOperation(name, collectionName)
                             .capped(createCollectionOptions.isCapped())
                             .sizeInBytes(createCollectionOptions.getSizeInBytes())
                             .autoIndex(createCollectionOptions.isAutoIndex())
                             .maxDocuments(createCollectionOptions.getMaxDocuments())
                             .usePowerOf2Sizes(createCollectionOptions.isUsePowerOf2Sizes())
                             .storageEngineOptions(asBson(createCollectionOptions.getStorageEngineOptions())));
    }

    @Override
    public Document executeCommand(final Object command) {
        return executeCommand(command, Document.class);
    }

    @Override
    public Document executeCommand(final Object command, final ReadPreference readPreference) {
        return executeCommand(command, readPreference, Document.class);
    }

    @Override
    public <T> T executeCommand(final Object command, final Class<T> clazz) {
        return executor.execute(new CommandWriteOperation<T>(getName(), asBson(command), options.getCodecRegistry().get(clazz)));
    }

    @Override
    public <T> T executeCommand(final Object command, final ReadPreference readPreference, final Class<T> clazz) {
        notNull("readPreference", readPreference);
        return executor.execute(new CommandReadOperation<T>(getName(), asBson(command), options.getCodecRegistry().get(clazz)),
                                readPreference);
    }

    private BsonDocument asBson(final Object document) {
        return BsonDocumentWrapper.asBsonDocument(document, options.getCodecRegistry());
    }
}
