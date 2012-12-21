package org.mongodb;

import org.bson.types.Document;

import java.util.List;

/**
 * Provides the functionality for a collection that is useful for administration, but not necessarily in the course
 * of normal use of a collection.
 */
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

    /**
     * @return true is this is a capped collection
     */
    boolean isCapped();
}
