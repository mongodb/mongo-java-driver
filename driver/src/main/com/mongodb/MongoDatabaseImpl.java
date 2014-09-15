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

import com.mongodb.client.DatabaseAdministration;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCollectionOptions;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoDatabaseOptions;
import com.mongodb.operation.CommandReadOperation;
import com.mongodb.operation.CommandWriteOperation;
import com.mongodb.operation.OperationExecutor;
import com.mongodb.operation.ReadOperation;
import com.mongodb.operation.WriteOperation;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.mongodb.Document;

import static com.mongodb.assertions.Assertions.notNull;

class MongoDatabaseImpl implements MongoDatabase {
    private final MongoDatabaseOptions options;
    private final String name;
    private final MongoClient client;
    private final DatabaseAdministration admin;

    public MongoDatabaseImpl(final String name, final MongoClient client, final MongoDatabaseOptions options) {
        this.name = name;
        this.client = client;
        this.options = options;
        this.admin = new DatabaseAdministrationImpl(name, client);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public MongoCollection<Document> getCollection(final String collectionName) {
        return getCollection(collectionName, MongoCollectionOptions.builder().build().withDefaults(options));
    }

    @Override
    public MongoCollection<Document> getCollection(final String collectionName, final MongoCollectionOptions options) {
        return getCollection(collectionName, Document.class, options);
    }

    @Override
    public <T> MongoCollection<T> getCollection(final String collectionName, final Class<T> clazz) {
        return getCollection(collectionName, clazz, MongoCollectionOptions.builder().build().withDefaults(options));
    }

    @Override
    public <T> MongoCollection<T> getCollection(final String collectionName, final Class<T> clazz, final MongoCollectionOptions options) {
        return new MongoCollectionImpl<T>(new MongoNamespace(name, collectionName), clazz, options.withDefaults(this.options),
                                          getOperationExecutor());
    }

    private OperationExecutor getOperationExecutor() {
        return new OperationExecutor() {
            @Override
            public <T> T execute(final ReadOperation<T> operation, final ReadPreference readPreference) {
                return client.execute(operation, readPreference);
            }

            @Override
            public <T> T execute(final WriteOperation<T> operation) {
                return client.execute(operation);
            }
        };
    }

    @Override
    public DatabaseAdministration tools() {
        return admin;
    }

    @Override
    public Document executeCommand(final Document command) {
        return client.execute(new CommandWriteOperation<Document>(getName(), wrap(command),
                                                                  options.getCodecRegistry().get(Document.class)));
    }

    @Override
    public Document executeCommand(final Document command, final ReadPreference readPreference) {
        notNull("readPreference", readPreference);
        return client.execute(new CommandReadOperation<Document>(getName(), wrap(command), options.getCodecRegistry().get(Document.class)),
                              readPreference);
    }

    @Override
    public MongoDatabaseOptions getOptions() {
        return options;
    }

    private BsonDocument wrap(final Document command) {
        return new BsonDocumentWrapper<Document>(command, options.getCodecRegistry().get(Document.class));
    }
}
