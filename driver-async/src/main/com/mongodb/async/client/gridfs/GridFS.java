package com.mongodb.async.client.gridfs;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.client.result.DeleteResult;
import org.bson.types.ObjectId;

import java.util.List;


public interface GridFS {

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
     * @param callback callback that is completed when the result is available
     */
    void find(Object filter, SingleResultCallback<List<GridFSFile>> callback);

    /**
     * Delete a GridFSFile by its id.
     *
     * @param id the id as ObjectId
     * @param callback callback that is completed when the file is deleted
     */
    void deleteOne(ObjectId id, SingleResultCallback<DeleteResult> callback);
}
