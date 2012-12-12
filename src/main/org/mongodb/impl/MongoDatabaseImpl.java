/**
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
 *
 */

package org.mongodb.impl;

import org.mongodb.MongoClient;
import org.mongodb.MongoDatabase;
import org.mongodb.MongoDocument;
import org.mongodb.ReadPreference;
import org.mongodb.WriteConcern;
import org.mongodb.operation.MongoCommandOperation;
import org.mongodb.result.CommandResult;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.serializers.MongoDocumentSerializer;

class MongoDatabaseImpl implements MongoDatabase {
    private final MongoClient client;
    private final String name;
    private final WriteConcern writeConcern;
    private ReadPreference readPreference;

    public MongoDatabaseImpl(final String name, final MongoClient client) {
        this(name, client, null);
    }

    public MongoDatabaseImpl(final String name, final MongoClient client, final WriteConcern writeConcern) {
        this.name = name;
        this.client = client;
        this.writeConcern = writeConcern;
    }

    @Override
    public String getName() {
        return name;
    }

    public MongoCollectionImpl<MongoDocument> getCollection(final String name) {
        return getTypedCollection(name, getSerializers(), new MongoDocumentSerializer(getSerializers()));
    }

    @Override
    public <T> MongoCollectionImpl<T> getTypedCollection(final String name,
                                                         final PrimitiveSerializers basePrimitiveSerializers,
                                                         final Serializer<T> serializer) {
        return new MongoCollectionImpl<T>(name, this, basePrimitiveSerializers, serializer, null, null);
    }

    @Override
    public CommandResult executeCommand(final MongoCommandOperation commandOperation) {
        return new CommandResult(client.getOperations().executeCommand(getName(), commandOperation, null));
    }

    @Override
    public MongoClient getClient() {
        return client;
    }

    public WriteConcern getWriteConcern() {
        if (writeConcern != null) {
            return writeConcern;
        }
        return getClient().getWriteConcern();
    }

    public ReadPreference getReadPreference() {
        if (readPreference != null) {
            return readPreference;
        }
        return getClient().getReadPreference();
    }

    public PrimitiveSerializers getSerializers() {
        return getClient().getPrimitiveSerializers();
    }
}
