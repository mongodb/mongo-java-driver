/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package org.mongodb.impl;

import org.bson.types.Document;
import org.mongodb.DatabaseAdmin;
import org.mongodb.MongoAsyncCollection;
import org.mongodb.MongoClient;
import org.mongodb.MongoCollectionOptions;
import org.mongodb.MongoDatabase;
import org.mongodb.MongoDatabaseOptions;
import org.mongodb.operation.MongoCommand;
import org.mongodb.result.CommandResult;
import org.mongodb.serialization.CollectibleSerializer;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.serializers.CollectibleDocumentSerializer;
import org.mongodb.serialization.serializers.DocumentSerializer;
import org.mongodb.serialization.serializers.ObjectIdGenerator;

class MongoDatabaseImpl implements MongoDatabase {
    private final MongoClient client;
    private final MongoDatabaseOptions options;
    private final String name;
    private final DatabaseAdmin admin;

    public MongoDatabaseImpl(final String name, final MongoClient client, final MongoDatabaseOptions options) {
        this.name = name;
        this.client = client;
        this.options = options;
        this.admin = new DatabaseAdminImpl(name, client.getOperations(), client.getOptions().getPrimitiveSerializers());
    }

    @Override
    public String getName() {
        return name;
    }

    public MongoCollectionImpl<Document> getCollection(final String collectionName) {
        return getCollection(collectionName, MongoCollectionOptions.builder().build());
    }

    @Override
    public MongoCollectionImpl<Document> getCollection(final String collectionName,
                                                       final MongoCollectionOptions options) {
        return getTypedCollection(collectionName, new CollectibleDocumentSerializer(
                options.withDefaults(this.options).getPrimitiveSerializers(), new ObjectIdGenerator()), options);
    }

    @Override
    public <T> MongoCollectionImpl<T> getTypedCollection(final String name, final CollectibleSerializer<T> serializer) {
        return getTypedCollection(name, serializer, MongoCollectionOptions.builder().build());
    }

    @Override
    public <T> MongoCollectionImpl<T> getTypedCollection(final String name, final CollectibleSerializer<T> serializer,
                                                         final MongoCollectionOptions options) {
        return new MongoCollectionImpl<T>(name, this, serializer, options.withDefaults(this.options));
    }

    @Override
    public MongoAsyncCollection<Document> getAsyncCollection(final String collectionName) {
        return new MongoAsyncCollectionImpl<Document>(getCollection(collectionName));
    }

    @Override
    public <T> MongoAsyncCollection<T> getAsyncTypedCollection(final String collectionName,
                                                               final PrimitiveSerializers primitiveSerializers,
                                                               final CollectibleSerializer<T> serializer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DatabaseAdmin admin() {
        return admin;
    }

    @Override
    public CommandResult executeCommand(final MongoCommand commandOperation) {
        return new CommandResult(client.getOperations().executeCommand(getName(), commandOperation,
                                                                       new DocumentSerializer(
                                                                               options.getPrimitiveSerializers())));
    }

    @Override
    public MongoClient getClient() {
        return client;
    }

    @Override
    public MongoDatabaseOptions getOptions() {
        return options;
    }
}
