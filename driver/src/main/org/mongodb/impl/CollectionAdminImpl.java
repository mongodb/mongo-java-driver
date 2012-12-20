package org.mongodb.impl;

import org.bson.types.Document;
import org.mongodb.CollectionAdmin;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoOperations;
import org.mongodb.OrderBy;
import org.mongodb.QueryFilterDocument;
import org.mongodb.WriteConcern;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.result.QueryResult;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.serializers.DocumentSerializer;

import java.util.List;
import java.util.Map;

public class CollectionAdminImpl implements CollectionAdmin {
    private static final MongoFind FIND_ALL = new MongoFind(new QueryFilterDocument());

    private final MongoOperations operations;
    private final String databaseName;
    //TODO: need to do something about these default serialisers, they're created everywhere
    private final DocumentSerializer documentSerializer;
    private String collectionName;

    CollectionAdminImpl(final MongoOperations operations, final PrimitiveSerializers primitiveSerializers,
                        final String databaseName, final String collectionName) {
        this.operations = operations;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.documentSerializer = new DocumentSerializer(primitiveSerializers);
    }

    @Override
    //TODO: need to support compound indexes
    public void ensureIndex(final String key, final OrderBy order) {
        // TODO: check for index ??
        //        final List<Document> indexes = getIndexes();

        final Document indexDetails = new Document("ns", databaseName + "." + collectionName);

        Index index = new Index(key, order.getIntRepresentation());
        indexDetails.append("name", generateIndexName(index));
        indexDetails.append("key", index);
        final MongoInsert<Document> insertIndexOperation = new MongoInsert<Document>(indexDetails);
        insertIndexOperation.writeConcern(WriteConcern.SAFE);

        //TODO: this may be shared with other methods in this class
        final MongoNamespace indexCollectionNamespace = new MongoNamespace(databaseName, "system.indexes");
        operations.insert(indexCollectionNamespace, insertIndexOperation, documentSerializer);
    }

    @Override
    public List<Document> getIndexes() {
        final MongoNamespace namespace = new MongoNamespace(databaseName, "system.indexes");
        final QueryResult<Document> systemCollection = operations.query(namespace, FIND_ALL, documentSerializer,
                                                                        documentSerializer);
        return systemCollection.getResults();
    }

    /**
     * Convenience method to generate an index name from the set of fields it is over.
     *
     * @param keys the names of the fields used in this index
     * @return a string representation of this index's fields
     */
    private static String generateIndexName(final Map<String, Object> keys) {
        final StringBuilder indexName = new StringBuilder();
        for (String keyNames : keys.keySet()) {
            if (indexName.length() != 0) {
                indexName.append('_');
            }
            indexName.append(keyNames).append('_');
            //is this ever anything other than an int?
            final Object ascOrDescValue = keys.get(keyNames);
            if (ascOrDescValue instanceof Number || ascOrDescValue instanceof String) {
                indexName.append(ascOrDescValue.toString().replace(' ', '_'));
            }
        }
        return indexName.toString();
    }

    private static final class Index extends Document {
        public Index(final String key, final int value) {
            super(key, value);
        }
    }

}
