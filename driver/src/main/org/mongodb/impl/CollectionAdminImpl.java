package org.mongodb.impl;

import org.bson.types.Document;
import org.mongodb.CollectionAdmin;
import org.mongodb.CommandDocument;
import org.mongodb.Index;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoOperations;
import org.mongodb.QueryFilterDocument;
import org.mongodb.WriteConcern;
import org.mongodb.operation.MongoCommandOperation;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.result.CommandResult;
import org.mongodb.result.QueryResult;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.serializers.DocumentSerializer;

import java.util.List;

import static org.mongodb.impl.ErrorHandling.handleErrors;

public class CollectionAdminImpl implements CollectionAdmin {
    private static final String NAMESPACE_KAY_NAME = "ns";

    private final MongoOperations operations;
    private final String databaseName;
    //TODO: need to do something about these default serialisers, they're created everywhere
    private final DocumentSerializer documentSerializer;
    private final MongoNamespace indexesNamespace;
    private final MongoNamespace collectionNamespace;
    private final CollStats collStatsCommand;
    private final MongoFind queryForCollectionNamespace;

    //TODO: pass in namespace
    CollectionAdminImpl(final MongoOperations operations, final PrimitiveSerializers primitiveSerializers,
                        final String databaseName, final String collectionName) {
        this.operations = operations;
        this.databaseName = databaseName;
        this.documentSerializer = new DocumentSerializer(primitiveSerializers);
        indexesNamespace = new MongoNamespace(this.databaseName, "system.indexes");
        collectionNamespace = new MongoNamespace(this.databaseName, collectionName);
        collStatsCommand = new CollStats(collectionNamespace.getCollectionName());
        queryForCollectionNamespace = new MongoFind(
                new QueryFilterDocument(NAMESPACE_KAY_NAME, collectionNamespace.getFullName()));
    }

    @Override
    public void ensureIndex(final Index index) {
        // TODO: check for index ??
        //        final List<Document> indexes = getIndexes();

        final Document indexDetails = new Document(NAMESPACE_KAY_NAME, collectionNamespace.getFullName());
        indexDetails.append("name", index.getName());
        indexDetails.append("key", index.toDocument());
        indexDetails.append("unique", index.isUnique());

        final MongoInsert<Document> insertIndexOperation = new MongoInsert<Document>(indexDetails);
        insertIndexOperation.writeConcern(WriteConcern.SAFE);

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
        CommandResult commandResult = new CommandResult(
                operations.executeCommand(databaseName, collStatsCommand, documentSerializer));
        handleErrors(commandResult, "Error getting collstats for '" + collectionNamespace.getFullName() + "'");

        Object capped = commandResult.getResponse().get("capped");
        return capped != null && ((Boolean) capped);
    }

    private final class CollStats extends MongoCommandOperation {
        private CollStats(final String collectionName) {
            super(new CommandDocument("collStats", collectionName));
        }
    }

}
