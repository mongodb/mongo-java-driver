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
import com.mongodb.operation.CreateCollectionOperation;
import com.mongodb.operation.DropDatabaseOperation;
import com.mongodb.operation.GetCollectionNamesOperation;
import com.mongodb.operation.RenameCollectionOperation;
import org.mongodb.CreateCollectionOptions;

import java.util.List;

import static com.mongodb.ReadPreference.primary;

/**
 * Runs the admin commands for a selected database.  This should be accessed from MongoDatabase.  The methods here are not implemented in
 * MongoDatabase in order to keep the API very simple, these should be the methods that are not commonly used by clients of the driver.
 */
class DatabaseAdministrationImpl implements DatabaseAdministration {

    private final String databaseName;
    private final MongoClient client;

    public DatabaseAdministrationImpl(final String databaseName, final MongoClient client) {
        this.databaseName = databaseName;
        this.client = client;
    }

    @Override
    public void drop() {
        client.execute(new DropDatabaseOperation(databaseName));
    }

    @Override
    public List<String> getCollectionNames() {
        return client.execute(new GetCollectionNamesOperation(databaseName), primary());
    }

    @Override
    public void createCollection(final String collectionName) {
        createCollection(new CreateCollectionOptions(collectionName));
    }

    @Override
    public void createCollection(final CreateCollectionOptions createCollectionOptions) {
        client.execute(new CreateCollectionOperation(databaseName, createCollectionOptions));
    }

    @Override
    public void renameCollection(final String oldCollectionName, final String newCollectionName) {
        client.execute(new RenameCollectionOperation(databaseName, oldCollectionName, newCollectionName, false));
    }

    @Override
    public void renameCollection(final String oldCollectionName, final String newCollectionName, final boolean dropTarget) {
        client.execute(new RenameCollectionOperation(databaseName, oldCollectionName, newCollectionName, dropTarget));
    }

}
