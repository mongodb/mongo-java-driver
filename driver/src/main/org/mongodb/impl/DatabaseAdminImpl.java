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

import org.bson.types.Document;
import org.mongodb.CreateCollectionOptions;
import org.mongodb.DatabaseAdmin;
import org.mongodb.MongoConnection;
import org.mongodb.MongoNamespace;
import org.mongodb.QueryFilterDocument;
import org.mongodb.ReadPreference;
import org.mongodb.command.Create;
import org.mongodb.command.DropDatabase;
import org.mongodb.command.RenameCollection;
import org.mongodb.command.RenameCollectionOptions;
import org.mongodb.operation.MongoFind;
import org.mongodb.result.CommandResult;
import org.mongodb.result.QueryResult;
import org.mongodb.serialization.Serializer;

import java.util.HashSet;
import java.util.Set;

import static org.mongodb.impl.ErrorHandling.handleErrors;

/**
 * Runs the admin commands for a selected database.  This should be accessed from MongoDatabase.  The methods here are
 * not implemented in MongoDatabase in order to keep the API very simple, these should be the methods that are not
 * commonly used by clients of the driver.
 */
public class DatabaseAdminImpl implements DatabaseAdmin {
    private static final DropDatabase DROP_DATABASE = new DropDatabase();
    private static final MongoFind FIND_ALL = new MongoFind(new QueryFilterDocument()).readPreference(ReadPreference.primary());

    private final String databaseName;
    private final Serializer<Document> documentSerializer;
    private final MongoConnection operations;

    public DatabaseAdminImpl(final String databaseName, final MongoConnection operations,
                             final Serializer<Document> documentSerializer) {
        this.databaseName = databaseName;
        this.operations = operations;
        this.documentSerializer = documentSerializer;
    }

    @Override
    public void drop() {
        //TODO: should inspect the CommandResult to make sure it went OK
        new CommandResult(operations.command(databaseName, DROP_DATABASE, documentSerializer));
    }

    @Override
    public Set<String> getCollectionNames() {
        final MongoNamespace namespacesCollection = new MongoNamespace(databaseName, "system.namespaces");
        final QueryResult<Document> query = operations.query(namespacesCollection, FIND_ALL,
                                                            documentSerializer, documentSerializer);

        final HashSet<String> collections = new HashSet<String>();
        final int lengthOfDatabaseName = databaseName.length();
        for (final Document namespace : query.getResults()) {
            final String collectionName = (String) namespace.get("name");
            if (!collectionName.contains("$")) {
                final String collectionNameWithoutDatabasePrefix = collectionName.substring(lengthOfDatabaseName + 1);
                collections.add(collectionNameWithoutDatabasePrefix);
            }
        }
        return collections;
    }

    @Override
    public void createCollection(final String collectionName) {
        createCollection(new CreateCollectionOptions(collectionName));
    }

    @Override
    public void createCollection(final CreateCollectionOptions createCollectionOptions) {
        final CommandResult commandResult = new CommandResult(
                operations.command(databaseName, new Create(createCollectionOptions), documentSerializer));
        handleErrors(commandResult);
    }

    @Override
    public void renameCollection(final String oldCollectionName, final String newCollectionName) {
        renameCollection(new RenameCollectionOptions(oldCollectionName, newCollectionName));
    }

    @Override
    public void renameCollection(final RenameCollectionOptions renameCollectionOptions) {
        final RenameCollection rename = new RenameCollection(renameCollectionOptions, databaseName);
        final CommandResult commandResult = operations.command("admin", rename, documentSerializer);
        handleErrors(commandResult);
    }
}
