package com.mongodb.async.client.gridfs;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.client.result.DeleteResult;
import org.bson.types.ObjectId;

import java.util.List;

/**
 * Implementation of GridFS - a specification for storing and retrieving files that exceed the BSON-document size limit of 16MB.
 */
public interface GridFS {

    /**
     * Creates the required index on the chunks collection used by this GridFS instance.
     *
     * The created index is a unique compound index with keys "files_id" and "n".
     * Calling this method is optional if the index has already been created manually.
     *
     * @param callback callback that is completed when the result of creating the index is available.
     */
    void createIndex(final SingleResultCallback<Void> callback);

    /**
     * Instantiate a new GridFSFile.
     *
     * @return GridFSFile
     */
    GridFSFile createFile();

    /**
     * Find a GridFSFile by its id.
     *
     * @param id the id as ObjectId
     * @param callback callback that is completed when the file is available
     */
    void findOne(ObjectId id, SingleResultCallback<GridFSFile> callback);

    /**
     * Finds all GridFS documents.
     *
     * @param filter a document describing the query filter, which may not be null. This can be of any type for which
     *               a {@code Codec} is registered
     * @param sort the sort criteria, which may be null.
     * @param callback callback that is completed when the result is available
     */
    void find(Object filter, Object sort, SingleResultCallback<List<GridFSFile>> callback);

    /**
     * Delete a GridFSFile by its id.
     *
     * @param id the id as ObjectId
     * @param callback callback that is completed when the file is deleted
     */
    void deleteOne(ObjectId id, SingleResultCallback<DeleteResult> callback);
}
