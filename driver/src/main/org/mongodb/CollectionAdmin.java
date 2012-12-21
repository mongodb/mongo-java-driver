package org.mongodb;

import org.bson.types.Document;

import java.util.List;

public interface CollectionAdmin {
    /**
     * @param index all the details of the index to add
     * @see Index
     * @see <a href="http://docs.mongodb.org/manual/reference/javascript/#db.collection.ensureIndex">ensureIndex</a>
     */
    void ensureIndex(Index index);

    /**
     * @return a MongoCollection containing all the indexes on this collection
     */
    List<Document> getIndexes();
}
