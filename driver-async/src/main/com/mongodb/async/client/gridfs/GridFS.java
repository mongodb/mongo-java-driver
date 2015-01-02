package com.mongodb.async.client.gridfs;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;


public class GridFS {

    private static final int DEFAULT_CHUNK_SIZE = 255 * 1024;
    private static final String DEFAULT_BUCKET_NAME = "FS";

    private final int chunkSize;
    private final MongoDatabase db;
    private final MongoCollection<GridFSFile> fileCollection;
    private final MongoCollection<Document> chunkCollection;
    private final String bucketName;

    public GridFS(final MongoDatabase db, final String bucket, final Integer chunkSize) {

        Objects.requireNonNull(db);
        this.db = db;

        this.bucketName = bucket == null ? DEFAULT_BUCKET_NAME : bucket;
        this.chunkSize = chunkSize == null ? DEFAULT_CHUNK_SIZE : chunkSize;

        this.fileCollection = this.db.getCollection(bucketName  + ".files", GridFSFile.class);
        this.chunkCollection = this.db.getCollection(bucketName + ".chunks");

        // from sync driver...
        // ensure standard indexes as long as collections are small
        fileCollection.count(new SingleResultCallback<Long>() {
            @Override
            public void onResult(final Long result, final Throwable t) {
                if (t != null) {
                    //TODO log error
                } else if (result < 1000) {
                    fileCollection.createIndex(
                            new Document("filename", 1).append("uploadDate", 1), new SingleResultCallback<Void>() {

                        @Override
                        public void onResult(final Void result, final Throwable t) {
                            //TODO log
                            if (t != null) {
                                //failed to create index
                            } else {
                                // index ok
                            }
                        }
                    });
                }
            }
        });

        chunkCollection.count(new SingleResultCallback<Long>() {
            @Override
            public void onResult(final Long result, final Throwable t) {
                if (t != null) {
                    //TODO log error
                } else if (result < 1000) {
                    chunkCollection.createIndex(new Document("files_id", 1).append("n", 1),
                            new CreateIndexOptions().unique(true), new SingleResultCallback<Void>() {

                        @Override
                        public void onResult(final Void result, final Throwable t) {
                            //TODO log
                            if (t != null) {
                                //failed to create index
                            } else {
                                // index ok
                            }
                        }
                    });
                }
            }
        });
    }

    /**
     * Instantiate a new GridFSFile.
     *
     * @return GridFSFile
     */
    public GridFSFile create() {
        GridFSFile gridFSFile = new GridFSFile(chunkSize);
        gridFSFile.setId(new ObjectId());
        gridFSFile.setUploadDate(new Date());
        return injectContext(gridFSFile);
    }

    /**
     * Find a GridFSFile by its id.
     *
     * @param id the id as ObjectId
     * @param callback callback that is completed when the file is available
     */
    public void find(final ObjectId id, final SingleResultCallback<GridFSFile> callback) {
        fileCollection.find(new Document("_id", id)).first(new SingleResultCallback<GridFSFile>() {
            @Override
            public void onResult(final GridFSFile result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    callback.onResult(injectContext(result), null);
                }
            }
        });
    }

    /**
     * List all GridFSFiles, restricted by the provided query.
     * @param query a document to limit the results that are returned
     * @param callback callback that will be completed when the result is available
     */
    public void listFiles(final Document query, final SingleResultCallback<List<GridFSFile>> callback) {
        fileCollection.find(query)
                .sort(new Document("filename", 1))
                .into(new ArrayList<GridFSFile>(), new SingleResultCallback<ArrayList<GridFSFile>>() {

            @Override
            public void onResult(final ArrayList<GridFSFile> result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    for (GridFSFile file : result) {
                        injectContext(file);
                    }
                    callback.onResult(result, null);
                }
            }
        });
    }

    /**
     * Delete a GridFSFile by its id.
     *
     * @param id the id as ObjectId
     * @param callback callback that is completed when the file is deleted
     */
    public void delete(final ObjectId id, final SingleResultCallback<DeleteResult> callback) {

        // delete the chunks
        chunkCollection.deleteMany(new Document("file_id", id), new SingleResultCallback<DeleteResult>() {
            @Override
            public void onResult(final DeleteResult result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    // delete the file
                    fileCollection.deleteOne(new Document("_id", id), callback);
                }
            }
        });
    }

    private GridFSFile injectContext(final GridFSFile gridFSFile) {
        gridFSFile.setGridFS(this);
        return gridFSFile;
    }

    /**
     * Get fileCollection.
     *
     * @return com.mongodb.async.client.MongoCollection<com.mongodb.async.client.gridfs.GridFSFile>
     */
    MongoCollection<GridFSFile> getFileCollection() {
        return fileCollection;
    }

    /**
     * Get chunkCollection.
     *
     * @return com.mongodb.async.client.MongoCollection<org.bson.Document>
     */
    MongoCollection<Document> getChunkCollection() {
        return chunkCollection;
    }

    /**
     * Get bucketName.
     *
     * @return java.lang.String
     */
    String getBucketName() {
        return bucketName;
    }

    /**
     * Get db.
     *
     * @return com.mongodb.async.client.MongoDatabase
     */
    MongoDatabase getDb() {
        return db;
    }
}
