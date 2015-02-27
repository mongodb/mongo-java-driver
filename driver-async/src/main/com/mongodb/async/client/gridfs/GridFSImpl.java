package com.mongodb.async.client.gridfs;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.FindIterable;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.configuration.CodecRegistryHelper;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * GridFS implementation.
 */
public class GridFSImpl implements GridFS {

    private static final int DEFAULT_CHUNK_SIZE = 255 * 1024;
    private static final String DEFAULT_BUCKET_NAME = "FS";

    private final int chunkSize;
    private final MongoDatabase database;
    private final MongoCollection<GridFSFile> filesCollection;
    private final MongoCollection<Document> chunksCollection;
    private final String bucketName;

    /**
     * Constructor.
     * @param database the MongoDatabase
     * @param bucket an optional bucket name
     * @param chunkSize an optional chunk size in kB that is used for all new GridFSFiles
     */
    public GridFSImpl(final MongoDatabase database, final String bucket, final Integer chunkSize) {

        this.database = database;
        CodecRegistry codecRegistry = this.database.getCodecRegistry();
        CodecRegistry gridFSCodecRegistry = CodecRegistryHelper.fromRegistries(codecRegistry,
                CodecRegistryHelper.fromCodec(new GridFSFileCodec(codecRegistry)));

        this.bucketName = bucket == null ? DEFAULT_BUCKET_NAME : bucket;
        this.chunkSize = chunkSize == null ? DEFAULT_CHUNK_SIZE : chunkSize;

        this.filesCollection = this.database.getCollection(bucketName  + ".files", GridFSFile.class)
                .withCodecRegistry(gridFSCodecRegistry);
        this.chunksCollection = this.database.getCollection(bucketName + ".chunks")
                .withCodecRegistry(gridFSCodecRegistry);

    }

    @Override
    public void createIndex(final SingleResultCallback<Void> callback) {

        chunksCollection.createIndex(new Document("files_id", 1).append("n", 1), new CreateIndexOptions().unique(true),
                new SingleResultCallback<Void>() {
                    @Override
                    public void onResult(final Void result, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            callback.onResult(null, null);
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
        filesCollection.find(new Document("_id", id)).first(new SingleResultCallback<GridFSFile>() {
            @Override
            public void onResult(final GridFSFile result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    if (result != null) {
                        callback.onResult(injectContext(result), null);
                    } else {
                        callback.onResult(null, null);
                    }
                }
            }
        });
    }

    @Override
    public void find(final Bson query, final Bson sort, final SingleResultCallback<List<GridFSFile>> callback) {

        FindIterable<GridFSFile> findFluent = filesCollection.find(query);

        if (sort != null) {
            findFluent.sort(sort);
        }

        findFluent.into(new ArrayList<GridFSFile>(), new SingleResultCallback<ArrayList<GridFSFile>>() {

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
        chunksCollection.deleteMany(new Document("file_id", id), new SingleResultCallback<DeleteResult>() {
            @Override
            public void onResult(final DeleteResult result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    // delete the file
                    filesCollection.deleteOne(new Document("_id", id), callback);
                }
            }
        });
    }

    private GridFSFile injectContext(final GridFSFile gridFSFile) {
        gridFSFile.setGridFS(this);
        return gridFSFile;
    }

    /**
     * Get filesCollection.
     *
     * @return com.mongodb.async.client.MongoCollection<com.mongodb.async.client.gridfs.GridFSFile>
     */
    MongoCollection<GridFSFile> getFilesCollection() {
        return filesCollection;
    }

    /**
     * Get chunksCollection.
     *
     * @return com.mongodb.async.client.MongoCollection<org.bson.Document>
     */
    MongoCollection<Document> getChunksCollection() {
        return chunksCollection;
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
