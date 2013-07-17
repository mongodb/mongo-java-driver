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

import org.mongodb.codecs.DocumentCodec;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.command.CollStats;
import org.mongodb.command.Drop;
import org.mongodb.command.DropIndex;
import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.operation.CommandResult;
import org.mongodb.operation.Find;
import org.mongodb.operation.Insert;
import org.mongodb.operation.InsertOperation;
import org.mongodb.operation.QueryOperation;
import org.mongodb.util.FieldHelpers;

import java.util.ArrayList;
import java.util.List;

import static org.mongodb.WriteConcern.ACKNOWLEDGED;

/**
 * Encapsulates functionality that is not part of the day-to-day use of a Collection.  For example, via this admin class
 * you can create indexes and drop the collection.
 */
class CollectionAdministrationImpl implements CollectionAdministration {
    private static final String NAMESPACE_KEY_NAME = "ns";

    private final MongoClientImpl client;
    private final MongoDatabase database;
    //TODO: need to do something about these default serialisers, they're created everywhere
    private final DocumentCodec documentCodec;
    private final MongoNamespace indexesNamespace;
    private final MongoNamespace collectionNamespace;
    private final Find queryForCollectionNamespace;

    private final CollStats collStatsCommand;
    private final Drop dropCollectionCommand;

    CollectionAdministrationImpl(final MongoClientImpl client,
                                 final PrimitiveCodecs primitiveCodecs,
                                 final MongoNamespace collectionNamespace,
                                 final MongoDatabase database) {
        this.client = client;
        this.database = database;
        this.documentCodec = new DocumentCodec(primitiveCodecs);
        indexesNamespace = new MongoNamespace(database.getName(), "system.indexes");
        this.collectionNamespace = collectionNamespace;
        collStatsCommand = new CollStats(collectionNamespace.getCollectionName());
        queryForCollectionNamespace = new Find(
                new Document(NAMESPACE_KEY_NAME, this.collectionNamespace.getFullName()))
                .readPreference(ReadPreference.primary());
        dropCollectionCommand = new Drop(this.collectionNamespace.getCollectionName());
    }

    @Override
    public void ensureIndex(final Index index) {
        final Document indexDetails = index.toDocument();
        indexDetails.append(NAMESPACE_KEY_NAME, collectionNamespace.getFullName());

        final Insert<Document> insertIndexOperation = new Insert<Document>(ACKNOWLEDGED, indexDetails);

        new InsertOperation<Document>(indexesNamespace, insertIndexOperation, documentCodec, client.getBufferProvider(),
                client.getSession(), false).execute();
    }

    @Override
    public List<Document> getIndexes() {
        List<Document> retVal = new ArrayList<Document>();
        MongoCursor<Document> cursor = new QueryOperation<Document>(indexesNamespace, queryForCollectionNamespace, documentCodec,
                documentCodec, client.getBufferProvider(), client.getSession(), false).execute();
        while (cursor.hasNext()) {
            retVal.add(cursor.next());
        }
        return retVal;
    }

    @Override
    public boolean isCapped() {
        final CommandResult commandResult = database.executeCommand(collStatsCommand);
        ErrorHandling.handleErrors(commandResult);

        return FieldHelpers.asBoolean(commandResult.getResponse().get("capped"));
    }

    @Override
    public Document getStatistics() {
        final CommandResult commandResult = database.executeCommand(collStatsCommand);
        ErrorHandling.handleErrors(commandResult);

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

        ErrorHandling.handleErrors(commandResult);
        //TODO: currently doesn't deal with errors
    }

    @Override
    public void dropIndexes() {
        final DropIndex dropIndex = new DropIndex(collectionNamespace.getCollectionName(), "*");
        final CommandResult commandResult = database.executeCommand(dropIndex);

        ErrorHandling.handleErrors(commandResult);
        //TODO: currently doesn't deal with errors
    }
}
