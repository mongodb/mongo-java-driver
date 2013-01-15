/*
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
 */

package org.mongodb.impl;

import org.bson.types.Document;
import org.mongodb.CollectionAdmin;
import org.mongodb.Index;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoOperations;
import org.mongodb.QueryFilterDocument;
import org.mongodb.WriteConcern;
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

    private final MongoOperations operations;
    private final String databaseName;
    //TODO: need to do something about these default serialisers, they're created everywhere
    private final DocumentSerializer documentSerializer;
    private final MongoNamespace indexesNamespace;
    private final MongoNamespace collectionNamespace;
    private final CollStats collStatsCommand;
    private final MongoFind queryForCollectionNamespace;

    //TODO: pass in namespace
    CollectionAdminImpl(final MongoOperations operations,
                        final PrimitiveSerializers primitiveSerializers,
                        final String databaseName, final String collectionName) {
        this.operations = operations;
        this.databaseName = databaseName;
        this.documentSerializer = new DocumentSerializer(primitiveSerializers);
        indexesNamespace = new MongoNamespace(this.databaseName, "system.indexes");
        collectionNamespace = new MongoNamespace(this.databaseName, collectionName);
        collStatsCommand = new CollStats(collectionNamespace.getCollectionName());
        queryForCollectionNamespace = new MongoFind(
                new QueryFilterDocument(NAMESPACE_KEY_NAME, collectionNamespace.getFullName()));
    }

    @Override
    public void ensureIndex(final Index index) {
        // TODO: encapsulate into commands
        final Document indexDetails = index.toDocument();
        indexDetails.append(NAMESPACE_KEY_NAME, collectionNamespace.getFullName());

        final MongoInsert<Document> insertIndexOperation = new MongoInsert<Document>(indexDetails);
        insertIndexOperation.writeConcern(WriteConcern.ACKNOWLEDGED);

        operations.insert(indexesNamespace, insertIndexOperation, documentSerializer);
    }

    @Override
    public List<Document> getIndexes() {
        final QueryResult<Document> systemCollection = operations.query(indexesNamespace, queryForCollectionNamespace,
                                                                        documentSerializer, documentSerializer);
        return systemCollection.getResults();
    }

    @Override
    public boolean isCapped() {
        final CommandResult commandResult = new CommandResult(operations.executeCommand(databaseName, collStatsCommand,
                                                                                  documentSerializer));
        handleErrors(commandResult, "Error getting collstats for '" + collectionNamespace.getFullName() + "'");

        return FieldHelpers.asBoolean(commandResult.getResponse().get("capped"));
    }

    @Override
    public Document getStatistics() {
        final CommandResult commandResult = new CommandResult(operations.executeCommand(databaseName, collStatsCommand,
                                                                                  documentSerializer));
        handleErrors(commandResult, "Error getting collstats for '" + collectionNamespace.getFullName() + "'");

        return commandResult.getResponse();
    }

    @Override
    public void drop() {
        final Drop command = new Drop(collectionNamespace.getCollectionName());
        operations.executeCommand(databaseName, command, documentSerializer);
        //ignores errors
        //TODO: which errors should be handled on drop?
    }
}
