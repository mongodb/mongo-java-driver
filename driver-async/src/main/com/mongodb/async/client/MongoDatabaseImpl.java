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
import com.mongodb.async.MongoFuture;
import com.mongodb.operation.CommandWriteOperation;
import org.bson.BsonDocumentWrapper;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DocumentCodec;

class MongoDatabaseImpl implements MongoDatabase {
    private final String name;
    private final MongoClientImpl client;
    private final MongoDatabaseOptions options;

    public MongoDatabaseImpl(final String name, final MongoClientImpl client, final MongoDatabaseOptions options) {
        this.name = name;
        this.client = client;
        this.options = options;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public MongoCollection<Document> getCollection(final String name) {
        return getCollection(name, MongoCollectionOptions.builder().build());
    }

    @Override
    public MongoCollection<Document> getCollection(final String name,
                                                   final MongoCollectionOptions mongoCollectionOptions) {
        return getCollection(name, new DocumentCodec(), mongoCollectionOptions.withDefaults(options));
    }


    @Override
    public <T> MongoCollection<T> getCollection(final String name, final Codec<T> codec,
                                                final MongoCollectionOptions mongoCollectionOptions) {
        return new MongoCollectionImpl<T>(new MongoNamespace(this.name, name), codec, mongoCollectionOptions.withDefaults(options), client);
    }

    @Override
    public MongoFuture<Document> executeCommand(final Document commandDocument) {
        return client.execute(new CommandWriteOperation<Document>(name, new BsonDocumentWrapper<Document>(commandDocument,
                                                                                                          options.getDocumentCodec()),
                                                                  new DocumentCodec()));
    }

    @Override
    public MongoDatabaseOptions getOptions() {
        return options;
    }

    @Override
    public DatabaseAdministration tools() {
        return new DatabaseAdministrationImpl(name, client);
    }
}
