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
 */

package org.mongodb;

import org.mongodb.codecs.CollectibleDocumentCodec;
import org.mongodb.codecs.ObjectIdGenerator;
import org.mongodb.command.Command;
import org.mongodb.operation.CommandOperation;
import org.mongodb.operation.CommandResult;

class MongoDatabaseImpl implements MongoDatabase {
    private final MongoDatabaseOptions options;
    private final String name;
    private final MongoClientImpl client;
    private final DatabaseAdministration admin;
    private final Codec<Document> documentCodec;

    public MongoDatabaseImpl(final String name, final MongoClientImpl client, final MongoDatabaseOptions options) {
        this.name = name;
        this.client = client;
        this.options = options;
        documentCodec = options.getDocumentCodec();
        this.admin = new DatabaseAdministrationImpl(name, client, documentCodec);
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
                                                       final MongoCollectionOptions operationOptions) {
        return getCollection(collectionName,
                            new CollectibleDocumentCodec(operationOptions
                                                              .withDefaults(options)
                                                              .getPrimitiveCodecs(),
                                                             new ObjectIdGenerator()),
                            operationOptions);
    }

    @Override
    public <T> MongoCollectionImpl<T> getCollection(final String collectionName,
                                                    final CollectibleCodec<T> codec) {
        return getCollection(collectionName, codec, MongoCollectionOptions.builder().build());
    }

    @Override
    public <T> MongoCollectionImpl<T> getCollection(final String collectionName,
                                                    final CollectibleCodec<T> codec,
                                                    final MongoCollectionOptions operationOptions) {
        return new MongoCollectionImpl<T>(collectionName, this, codec, operationOptions.withDefaults(options), client);
    }

    @Override
    public DatabaseAdministration tools() {
        return admin;
    }

    @Override
    public CommandResult executeCommand(final Command commandOperation) {
        commandOperation.readPreferenceIfAbsent(options.getReadPreference());
        return new CommandOperation(getName(), commandOperation, documentCodec, client.getCluster().getDescription(),
                client.getBufferProvider(), client.getSession(), false).execute();
    }

//    @Override
//    public MongoClient getClient() {
//        return null;
//    }

    @Override
    public MongoDatabaseOptions getOptions() {
        return options;
    }
}
