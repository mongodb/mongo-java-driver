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
import org.mongodb.result.QueryResult;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.serializers.DocumentSerializer;

import java.util.List;

public class CollectionAdminImpl implements CollectionAdmin {
    private static final MongoFind FIND_ALL = new MongoFind(new QueryFilterDocument());

    private final MongoOperations operations;
    private final String databaseName;
    //TODO: need to do something about these default serialisers, they're created everywhere
    private final DocumentSerializer documentSerializer;
    private final String collectionName;
    private final MongoNamespace indexesNamespace;

    CollectionAdminImpl(final MongoOperations operations, final PrimitiveSerializers primitiveSerializers,
                        final String databaseName, final String collectionName) {
        this.operations = operations;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.documentSerializer = new DocumentSerializer(primitiveSerializers);
        indexesNamespace = new MongoNamespace(this.databaseName, "system.indexes");
    }

    @Override
    public void ensureIndex(final Index index) {
        // TODO: check for index ??
        //        final List<Document> indexes = getIndexes();

        final Document indexDetails = new Document("ns", databaseName + "." + collectionName);
        indexDetails.append("name", index.getName());
        indexDetails.append("key", index.getAsDocument());
        indexDetails.append("unique", index.isUnique());

        final MongoInsert<Document> insertIndexOperation = new MongoInsert<Document>(indexDetails);
        insertIndexOperation.writeConcern(WriteConcern.SAFE);

        operations.insert(indexesNamespace, insertIndexOperation, documentSerializer);
    }

    @Override
    public List<Document> getIndexes() {
        final QueryResult<Document> systemCollection = operations.query(indexesNamespace, FIND_ALL, documentSerializer,
                                                                        documentSerializer);
        return systemCollection.getResults();
    }

}
