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

package org.mongodb.impl;

import org.mongodb.Document;
import org.mongodb.DatabaseAdmin;
import org.mongodb.MongoCollectionOptions;
import org.mongodb.MongoConnector;
import org.mongodb.MongoDatabase;
import org.mongodb.MongoDatabaseOptions;
import org.mongodb.command.MongoCommand;
import org.mongodb.result.CommandResult;
import org.mongodb.serialization.CollectibleSerializer;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.serializers.CollectibleDocumentSerializer;
import org.mongodb.serialization.serializers.ObjectIdGenerator;

class MongoDatabaseImpl implements MongoDatabase {
    private final MongoDatabaseOptions options;
    private final String name;
    private final MongoConnector connector;
    private final DatabaseAdmin admin;
    private final Serializer<Document> documentSerializer;

    public MongoDatabaseImpl(final String name, final MongoConnector connector, final MongoDatabaseOptions options) {
        this.name = name;
        this.connector = connector;
        this.options = options;
        documentSerializer = options.getDocumentSerializer();
        this.admin = new DatabaseAdminImpl(name, connector, documentSerializer);
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
                            new CollectibleDocumentSerializer(operationOptions
                                                              .withDefaults(options)
                                                              .getPrimitiveSerializers(),
                                                             new ObjectIdGenerator()),
                            operationOptions);
    }

    @Override
    public <T> MongoCollectionImpl<T> getCollection(final String collectionName,
                                                    final CollectibleSerializer<T> serializer) {
        return getCollection(collectionName, serializer, MongoCollectionOptions.builder().build());
    }

    @Override
    public <T> MongoCollectionImpl<T> getCollection(final String collectionName,
                                                    final CollectibleSerializer<T> serializer,
                                                    final MongoCollectionOptions operationOptions) {
        return new MongoCollectionImpl<T>(collectionName, this, serializer, operationOptions.withDefaults(options),
                connector);
    }

    @Override
    public DatabaseAdmin tools() {
        return admin;
    }

    @Override
    public CommandResult executeCommand(final MongoCommand commandOperation) {
        commandOperation.readPreferenceIfAbsent(options.getReadPreference());
        return new CommandResult(connector.command(getName(), commandOperation, documentSerializer));
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
