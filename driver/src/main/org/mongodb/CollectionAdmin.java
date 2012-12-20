package org.mongodb;

import org.bson.types.Document;
import org.mongodb.impl.CollectionAdminImpl;

import java.util.List;

public interface CollectionAdmin {
    /**
     * @param key the field to add the index to
     * @see <a href="http://docs.mongodb.org/manual/reference/javascript/#db.collection.ensureIndex">ensureIndex</a>
     */
    void ensureIndex(String key, CollectionAdminImpl.Order order);

    /**
     * @return a MongoCollection containing all the indexes on this collection
     */
    List<Document> getIndexes();
}
