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
import org.mongodb.CollectionAdmin;
import org.mongodb.Index;
import org.mongodb.MongoConnector;
import org.mongodb.MongoDatabase;
import org.mongodb.MongoNamespace;
import org.mongodb.ReadPreference;
import org.mongodb.WriteConcern;
import org.mongodb.command.CollStats;
import org.mongodb.command.Drop;
import org.mongodb.command.DropIndex;
import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.result.CommandResult;
import org.mongodb.result.QueryResult;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.serializers.DocumentSerializer;
import org.mongodb.util.FieldHelpers;

import java.util.List;

import static org.mongodb.impl.ErrorHandling.handleErrors;

/**
 * Encapsulates functionality that is not part of the day-to-day use of a Collection.  For example, via this admin class
 * you can create indexes and drop the collection.
 */
public class CollectionAdminImpl implements CollectionAdmin {
    private static final String NAMESPACE_KEY_NAME = "ns";

    private final MongoConnector connector;
    private final MongoDatabase database;
    //TODO: need to do something about these default serialisers, they're created everywhere
    private final DocumentSerializer documentSerializer;
    private final MongoNamespace indexesNamespace;
    private final MongoNamespace collectionNamespace;
    private final MongoFind queryForCollectionNamespace;

    private final CollStats collStatsCommand;
    private final Drop dropCollectionCommand;

    CollectionAdminImpl(final MongoConnector connector,
                        final PrimitiveSerializers primitiveSerializers,
                        final MongoNamespace collectionNamespace,
                        final MongoDatabase database) {
        this.connector = connector;
        this.database = database;
        this.documentSerializer = new DocumentSerializer(primitiveSerializers);
        indexesNamespace = new MongoNamespace(database.getName(), "system.indexes");
        this.collectionNamespace = collectionNamespace;
        collStatsCommand = new CollStats(collectionNamespace.getCollectionName());
        queryForCollectionNamespace = new MongoFind(
                new Document(NAMESPACE_KEY_NAME, this.collectionNamespace.getFullName()))
                .readPreference(ReadPreference.primary());
        dropCollectionCommand = new Drop(this.collectionNamespace.getCollectionName());
    }

    @Override
    public void ensureIndex(final Index index) {
        final Document indexDetails = index.toDocument();
        indexDetails.append(NAMESPACE_KEY_NAME, collectionNamespace.getFullName());

        final MongoInsert<Document> insertIndexOperation = new MongoInsert<Document>(indexDetails);
        insertIndexOperation.writeConcern(WriteConcern.ACKNOWLEDGED);

        connector.insert(indexesNamespace, insertIndexOperation, documentSerializer);
    }

    @Override
    public List<Document> getIndexes() {
        final QueryResult<Document> systemCollection = connector.query(indexesNamespace, queryForCollectionNamespace,
                documentSerializer, documentSerializer);
        return systemCollection.getResults();
    }

    @Override
    public boolean isCapped() {
        final CommandResult commandResult = database.executeCommand(collStatsCommand);
        handleErrors(commandResult);

        return FieldHelpers.asBoolean(commandResult.getResponse().get("capped"));
    }

    @Override
    public Document getStatistics() {
        final CommandResult commandResult = database.executeCommand(collStatsCommand);
        handleErrors(commandResult);

        return commandResult.getResponse();
    }

    @Override
    public void drop() {
        try {
            database.executeCommand(dropCollectionCommand);
        } catch (MongoCommandFailureException e) {
            if (!e.getCommandResult().getErrorMessage().equals("ns not found")) {
                throw e;
            }
        }
    }

    @Override
    public void dropIndex(final Index index) {
        final DropIndex dropIndex = new DropIndex(collectionNamespace.getCollectionName(), index.getName());
        final CommandResult commandResult = database.executeCommand(dropIndex);

        handleErrors(commandResult);
        //TODO: currently doesn't deal with errors
    }

    @Override
    public void dropIndexes() {
        final DropIndex dropIndex = new DropIndex(collectionNamespace.getCollectionName(), "*");
        final CommandResult commandResult = database.executeCommand(dropIndex);

        handleErrors(commandResult);
        //TODO: currently doesn't deal with errors
    }
}
