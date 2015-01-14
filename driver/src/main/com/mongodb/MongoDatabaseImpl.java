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

import com.mongodb.client.ListCollectionsFluent;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.operation.CommandReadOperation;
import com.mongodb.operation.CommandWriteOperation;
import com.mongodb.operation.CreateCollectionOperation;
import com.mongodb.operation.DropDatabaseOperation;
import com.mongodb.operation.OperationExecutor;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;

import static com.mongodb.assertions.Assertions.notNull;

class MongoDatabaseImpl implements MongoDatabase {
    private final String name;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;
    private final WriteConcern writeConcern;
    private final OperationExecutor executor;

    MongoDatabaseImpl(final String name, final CodecRegistry codecRegistry, final ReadPreference readPreference,
                      final WriteConcern writeConcern, final OperationExecutor executor) {
        this.name = notNull("name", name);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.executor = notNull("executor", executor);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public MongoDatabase withCodecRegistry(final CodecRegistry codecRegistry) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, executor);
    }

    @Override
    public MongoDatabase withReadPreference(final ReadPreference readPreference) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, executor);
    }

    @Override
    public MongoDatabase withWriteConcern(final WriteConcern writeConcern) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, executor);
    }

    @Override
    public MongoCollection<Document> getCollection(final String collectionName) {
        return getCollection(collectionName, Document.class);
    }

    @Override
    public <T> MongoCollection<T> getCollection(final String collectionName, final Class<T> clazz) {
        return new MongoCollectionImpl<T>(new MongoNamespace(name, collectionName), clazz, codecRegistry, readPreference, writeConcern,
                executor);
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
        return executor.execute(new CommandWriteOperation<T>(getName(), asBson(command), codecRegistry.get(clazz)));
    }

    @Override
    public <T> T executeCommand(final Object command, final ReadPreference readPreference, final Class<T> clazz) {
        notNull("readPreference", readPreference);
        return executor.execute(new CommandReadOperation<T>(getName(), asBson(command), codecRegistry.get(clazz)),
                readPreference);
    }

    @Override
    public void dropDatabase() {
        executor.execute(new DropDatabaseOperation(name));
    }

    @Override
    public MongoIterable<String> listCollectionNames() {
        return listCollections().map(new Function<Document, String>() {
            @Override
            public String apply(final Document result) {
                return (String) result.get("name");
            }
        });
    }

    @Override
    public ListCollectionsFluent<Document> listCollections() {
        return listCollections(Document.class);
    }

    @Override
    public <C> ListCollectionsFluent<C> listCollections(final Class<C> clazz) {
        return new ListCollectionsFluentImpl<C>(name, clazz, codecRegistry, ReadPreference.primary(), executor);
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

    private BsonDocument asBson(final Object document) {
        return BsonDocumentWrapper.asBsonDocument(document, codecRegistry);
    }
}
