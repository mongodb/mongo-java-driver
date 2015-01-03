package com.mongodb.async.client.gridfs;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.lang.String.format;

public class GridFSImpl implements GridFS {

    private static final int DEFAULT_CHUNK_SIZE = 255 * 1024;
    private static final String DEFAULT_BUCKET_NAME = "FS";
    private static final Logger LOGGER = Loggers.getLogger("async.client.gridfs");
    private static final int CREATE_INDEX_IF_LESS_DOCUMENTS = 1000;

    private final int chunkSize;
    private final MongoDatabase database;
    private final MongoCollection<GridFSFile> fileCollection;
    private final MongoCollection<Document> chunkCollection;
    private final String bucketName;

    public GridFSImpl(final MongoDatabase database, final String bucket, final Integer chunkSize) {

        this.database = database;

        this.bucketName = bucket == null ? DEFAULT_BUCKET_NAME : bucket;
        this.chunkSize = chunkSize == null ? DEFAULT_CHUNK_SIZE : chunkSize;

        this.fileCollection = this.database.getCollection(bucketName  + ".files", GridFSFile.class);
        this.chunkCollection = this.database.getCollection(bucketName + ".chunks");

        // from sync driver...
        // ensure standard indexes as long as collections are small
        createIndexIfApplicable(fileCollection, CREATE_INDEX_IF_LESS_DOCUMENTS,
                new Document("filename", 1).append("uploadDate", 1), null);
        createIndexIfApplicable(chunkCollection, CREATE_INDEX_IF_LESS_DOCUMENTS,
                new Document("files_id", 1).append("n", 1), new CreateIndexOptions().unique(true));

    }

    /**
     * Creates the provided index on the collection if there are not more or equal number of documents in the collection
     * than the provided limit.
     *
     * @param collection the collection to create the index on
     * @param documentLimit the maximum number of documents in the collection
     * @param index the index to create
     * @param options additional index options
     */
    private void createIndexIfApplicable(final MongoCollection collection, final int documentLimit,
                                         final Document index, final CreateIndexOptions options) {
        collection.count(new SingleResultCallback<Long>() {
            @Override
            public void onResult(final Long result, final Throwable t) {
                if (t != null) {
                    LOGGER.warn(format("Failed to determine count on file collection %s: %s",
                            fileCollection.getNamespace().getCollectionName(),
                            t.getMessage()));
                } else if (result < documentLimit) {

                    SingleResultCallback<Void> callback = new SingleResultCallback<Void>() {
                        @Override
                        public void onResult(final Void result, final Throwable t) {
                            if (t != null) {
                                LOGGER.warn(format("Failed to create index on file collection %s: %s",
                                        fileCollection.getNamespace().getCollectionName(),
                                        t.getMessage()));
                            }
                        }
                    };

                    if (options == null) {
                        fileCollection.createIndex(index, callback);
                    } else {
                        fileCollection.createIndex(index, options, callback);
                    }

                } else {
                    LOGGER.debug(format("Skipping creation of indexes on collection %s as the collection contains too "
                            + "many documents", fileCollection.getNamespace().getCollectionName()));
                }
            }
        });
    }

    @Override
    public GridFSFile createFile() {
        GridFSFile gridFSFile = new GridFSFile(chunkSize);
        gridFSFile.setId(new ObjectId());
        gridFSFile.setUploadDate(new Date());
        return injectContext(gridFSFile);
    }

    @Override
    public void findOne(final ObjectId id, final SingleResultCallback<GridFSFile> callback) {
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

    @Override
    public void find(final Object query, final SingleResultCallback<List<GridFSFile>> callback) {
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

    @Override
    public void deleteOne(final ObjectId id, final SingleResultCallback<DeleteResult> callback) {

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
     * Get database.
     *
     * @return com.mongodb.async.client.MongoDatabase
     */
    MongoDatabase getDatabase() {
        return database;
    }
}
