package org.mongodb.file;

import org.bson.types.ObjectId;
import org.mongodb.Document;
import org.mongodb.MongoCursor;
import org.mongodb.MongoView;
import org.mongodb.file.common.MongoFileConstants;

/**
 * This call holds all of the generic query methods and lookups methods for
 * metadata type lookups
 * 
 * @author David Buschman
 * 
 */
public class MongoFileQuery {

    private MongoFileStore store;

    /* package */MongoFileQuery(final MongoFileStore store) {

        this.store = store;
    }

    // --------------------------
    // ------ reading -------
    // --------------------------

    /**
     * finds one file matching the given id. Equivalent to findOne(id)
     * 
     * @param id
     * @return the MongoFile object
     * @throws MongoException
     */
    public MongoFile find(final ObjectId id) {

        Document one = this.store.getFilesCollection().find(new Document("_id", id)).getOne();
        if (one == null) {
            return null;
        }
        return new MongoFile(store, one);
    }

    /**
     * finds a list of files matching the given filename
     * 
     * @param filename
     * @return the MongoFileCursor object
     * @throws MongoException
     */
    public MongoFileCursor find(final String filename) {

        return find(filename, null);
    }

    /**
     * finds a list of files matching the given filename
     * 
     * @param filename
     * @param sort
     * @return the MongoFileCursor object
     * @throws MongoException
     */
    public MongoFileCursor find(final String filename, final Document sort) {

        return find(new Document(MongoFileConstants.filename.toString(), filename), sort);
    }

    /**
     * finds a list of files matching the given query
     * 
     * @param query
     * @return the MongoFileCursor object
     * @throws MongoException
     */
    public MongoFileCursor find(final Document query) {

        return find(query, null);
    }

    /**
     * finds a list of files matching the given query
     * 
     * @param query
     * @param sort
     * @return the MongoFileCursor object
     * @throws MongoException
     */
    public MongoFileCursor find(final Document query, final Document sort) {

        MongoView<Document> c = store.getFilesCollection().find(query);
        if (sort != null) {
            c.sort(sort);
        }

        MongoCursor<Document> cursor = c.get();
        return new MongoFileCursor(store, cursor);
    }

    /**
     * Gets the list of files stored in this mongoFS, sorted by filename.
     * 
     * @return the MongoFileCursor object
     */
    public MongoFileCursor getFileList() {

        return getFileList(null);
    }

    /**
     * Gets a filtered list of files stored in this mongoFS, sorted by filename.
     * 
     * @param query
     *            filter to apply
     * @return the MongoFileCursor object
     */
    public MongoFileCursor getFileList(final Document query) {

        return new MongoFileCursor(store, store.getFilesCollection().find(query).get());
    }

    /**
     * Gets a sorted, filtered list of files stored in this mongoFS.
     * 
     * @param query
     *            filter to apply
     * @param sort
     *            sorting to apply
     * @return the MongoFileCursor object
     */
    public MongoFileCursor getFileList(final Document query, final Document sort) {

        return new MongoFileCursor(store, store.getFilesCollection().find(query).sort(sort).get());
    }

}
