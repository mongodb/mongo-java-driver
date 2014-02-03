/*
 * Copyright (c) 2008 - 2014 MongoDB, Inc.
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

import org.mongodb.operation.CreateIndexesOperation;
import org.mongodb.operation.DropCollectionOperation;
import org.mongodb.operation.DropIndexOperation;
import org.mongodb.operation.GetIndexesOperation;
import org.mongodb.util.FieldHelpers;

import java.util.List;

/**
 * Encapsulates functionality that is not part of the day-to-day use of a Collection.  For example, via this admin class you can create
 * indexes and drop the collection.
 */
class CollectionAdministrationImpl implements CollectionAdministration {

    private final MongoClientImpl client;
    private final MongoDatabase database;
    private final MongoNamespace collectionNamespace;

    private final Document collStatsCommand;

    CollectionAdministrationImpl(final MongoClientImpl client,
                                 final MongoNamespace collectionNamespace,
                                 final MongoDatabase database) {
        this.client = client;
        this.database = database;
        this.collectionNamespace = collectionNamespace;
        collStatsCommand = new Document("collStats", collectionNamespace.getCollectionName());
    }

    @Override
    public void createIndexes(final List<Index> indexes) {
        new CreateIndexesOperation(indexes, collectionNamespace, client.getCluster().getDescription(),
                                 client.getSession(), false, client.getBufferProvider()).execute();
    }

    @Override
    public List<Document> getIndexes() {
        return new GetIndexesOperation(collectionNamespace, client.getBufferProvider(), client.getSession()).execute();
    }

    @Override
    public boolean isCapped() {
        CommandResult commandResult = database.executeCommand(collStatsCommand, null);
        ErrorHandling.handleErrors(commandResult);

        return FieldHelpers.asBoolean(commandResult.getResponse().get("capped"));
    }

    @Override
    public Document getStatistics() {
        CommandResult commandResult = database.executeCommand(collStatsCommand, null);
        ErrorHandling.handleErrors(commandResult);

        return commandResult.getResponse();
    }

    @Override
    public void drop() {
        new DropCollectionOperation(collectionNamespace, client.getBufferProvider(), client.getSession(), false).execute();
    }

    @Override
    public void dropIndex(final Index index) {
        new DropIndexOperation(collectionNamespace, index.getName(), client.getBufferProvider(), client.getSession(), false).execute();
    }

    @Override
    public void dropIndexes() {
        new DropIndexOperation(collectionNamespace, "*", client.getBufferProvider(), client.getSession(), false).execute();
    }
}
