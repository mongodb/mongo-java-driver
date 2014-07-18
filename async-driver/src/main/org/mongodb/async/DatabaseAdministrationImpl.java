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

import com.mongodb.operation.CreateCollectionOperation;
import com.mongodb.operation.DropDatabaseOperation;
import com.mongodb.operation.GetCollectionNamesOperation;
import com.mongodb.operation.RenameCollectionOperation;
import org.mongodb.CreateCollectionOptions;
import org.mongodb.MongoFuture;

import java.util.List;

import static com.mongodb.ReadPreference.primary;

/**
 * The administrative commands that can be run against a selected database.  Application developers should not normally need to call these
 * methods.
 *
 * @since 3.0
 */
public class DatabaseAdministrationImpl implements DatabaseAdministration {

    private final String databaseName;
    private final MongoClientImpl client;

    public DatabaseAdministrationImpl(final String databaseName, final MongoClientImpl client) {
        this.databaseName = databaseName;
        this.client = client;
    }

    @Override
    public MongoFuture<Void> drop() {
        return client.execute(new DropDatabaseOperation(databaseName));
    }

    @Override
    public MongoFuture<List<String>> getCollectionNames() {
        return client.execute(new GetCollectionNamesOperation(databaseName), primary());
    }

    @Override
    public MongoFuture<Void> createCollection(final String collectionName) {
        return createCollection(new CreateCollectionOptions(collectionName));
    }

    @Override
    public MongoFuture<Void> createCollection(final CreateCollectionOptions createCollectionOptions) {
        return client.execute(new CreateCollectionOperation(databaseName, createCollectionOptions));
    }

    @Override
    public MongoFuture<Void> renameCollection(final String oldCollectionName, final String newCollectionName) {
        return client.execute(new RenameCollectionOperation(databaseName, oldCollectionName, newCollectionName, false));
    }

    @Override
    public MongoFuture<Void> renameCollection(final String oldCollectionName, final String newCollectionName, final boolean dropTarget) {
        return client.execute(new RenameCollectionOperation(databaseName, oldCollectionName, newCollectionName, dropTarget));
    }
}
