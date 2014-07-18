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

package org.mongodb.async;


import com.mongodb.codecs.DocumentCodec;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.Codec;
import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MongoCollectionOptions;
import org.mongodb.MongoDatabaseOptions;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.operation.CommandWriteOperation;

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
        return getCollection(name, MongoCollectionOptions.builder().build().withDefaults(options));
    }

    @Override
    public MongoCollection<Document> getCollection(final String name,
                                                   final MongoCollectionOptions options) {
        return getCollection(name, new DocumentCodec(), options);
    }


    @Override
    public <T> MongoCollection<T> getCollection(final String name, final Codec<T> codec, final MongoCollectionOptions options) {
        return new MongoCollectionImpl<T>(new MongoNamespace(this.name, name), codec, options, client);
    }

    @Override
    public MongoFuture<CommandResult> executeCommand(final Document commandDocument) {
        return client.execute(new CommandWriteOperation(name, new BsonDocumentWrapper<Document>(commandDocument,
                                                                                                options.getDocumentCodec())
        ));
    }

    @Override
    public DatabaseAdministration tools() {
        return new DatabaseAdministrationImpl(name, client);
    }
}
