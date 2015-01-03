package com.mongodb.async.client.gridfs;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoCollection;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class GridFSFile {

    private ObjectId id;
    //TODO aliases
    private int chunkSize;
    private String contentType;
    private String filename;
    private long length = 0;
    private String md5;
    private Date uploadDate;
    private Map<String, Object> metadata;

    private GridFSImpl gridFS;
    /**
     * The actual chunks, used when creating a new file as we probably don't know the filesize until we are done...
     * However, we know the chunksize, so we can initialize a chunk and add more if needed...
     */
    private List<byte[]> chunks;

    /**
     * the curent position in the last chunk. If '-1' the file is no longer writeable
     */
    private int positionInChunk = 0;

    GridFSFile() { }

    GridFSFile(final int chunkSize) {
        this.chunkSize = chunkSize;
    }

    /**
     * Get id.
     *
     * @return the id
     */
    public ObjectId getId() {
        return id;
    }

    /**
     * Get chunkSize.
     *
     * @return the chunkSize
     */
    public int getChunkSize() {
        return chunkSize;
    }

    /**
     * Set the md5.
     *
     * @param md5 the md5
     */
    public void setMd5(final String md5) {
        this.md5 = md5;
    }

    /**
     * Get contentType.
     *
     * @return the contentType
     */
    public String getContentType() {
        return contentType;
    }

    /**
     * Get filename.
     *
     * @return the filename
     */
    public String getFilename() {
        return filename;
    }

    /**
     * Get length.
     *
     * @return the length
     */
    public long getLength() {
        return length;
    }

    /**
     * Set the contentType.
     *
     * @param contentType the contentType
     */
    public void setContentType(final String contentType) {
        this.contentType = contentType;
    }

    /**
     * Set the length.
     *
     * @param length the length
     */
    void setLength(final long length) {
        this.length = length;
    }

    /**
     * Set the filename.
     *
     * @param filename the filename
     */
    public void setFilename(final String filename) {
        this.filename = filename;
    }

    /**
     * Get md5.
     *
     * @return the md5
     */
    public String getMd5() {
        return md5;
    }

    /**
     * Get uploadDate.
     *
     * @return the uploadDate
     */
    public Date getUploadDate() {
        return uploadDate;
    }

    /**
     * Set the uploadDate.
     *
     * @param uploadDate the uploadDate
     */
    public void setUploadDate(final Date uploadDate) {
        this.uploadDate = uploadDate;
    }

    /**
     * Set the metadata.
     *
     * @param metadata the metadata
     */
    public void setMetadata(final Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    /**
     * Get metadata.
     *
     * @return the metadata
     */
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    /**
     * Set the id.
     *
     * @param id the id
     */
    public void setId(final ObjectId id) {
        this.id = id;
    }

    /**
     * Set the gridFS.
     *
     * @param gridFS the gridFS
     */
    void setGridFS(final GridFSImpl gridFS) {
        this.gridFS = gridFS;
    }

    /**
     * Append an array of bytes to this GridFSFile.
     *
     * @param bytes the bytes to append
     */
    public void append(final byte[] bytes) {

        if (positionInChunk == -1) {
            throw new RuntimeException("Data can not be appended after file has been saved");
        }

        if (chunks == null) {
            chunks = new ArrayList<byte[]>();
            chunks.add(new byte[chunkSize]);
        }

        int bytesToWrite = bytes.length;

        while (bytesToWrite > 0) {

            if (positionInChunk >= chunkSize) {
                chunks.add(new byte[chunkSize]);
                positionInChunk = 0;
            }

            byte[] currentChunk = chunks.get(chunks.size() - 1);
            int bytesToWriteToChunk = Math.min(chunkSize - positionInChunk, bytesToWrite);
            System.arraycopy(bytes, bytes.length - bytesToWrite, currentChunk, positionInChunk, bytesToWriteToChunk);
            bytesToWrite -= bytesToWriteToChunk;
            length += bytesToWriteToChunk;
            positionInChunk += bytesToWriteToChunk;

        }
    }

    /**
     * Insert a new GridFSFile.
     *
     * @param callback the callback that is completed once the insert has completed
     */
    public void insert(final SingleResultCallback<Void> callback) {

        if (chunks == null) {
            throw new RuntimeException("GridFSFile has no data!");
        }

        final MongoCollection<GridFSFile> fileCollection = gridFS.getFileCollection();
        final MongoCollection<Document> chunkCollection = gridFS.getChunkCollection();

        // the last chunk probably has unused bytes at the end... fix that
        if (positionInChunk > 0) {
            chunks.set(chunks.size() - 1, Arrays.copyOf(chunks.get(chunks.size() - 1), positionInChunk));
            positionInChunk = -1;
        }

        // prepare the chunk documents
        int numberOfChunks = chunks.size();
        List<Document> documents = new ArrayList<Document>(numberOfChunks);

        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            callback.onResult(null, e);
            return;
        }

        for (int i = 0; i < numberOfChunks; i++) {

            byte[] bytes = chunks.get(i);
            md.update(bytes);

            Document document = new Document();
            document.put("files_id", this.id);
            document.put("n", i);
            document.put("data", bytes);
            documents.add(document);
        }

        // calculate MD5 string
        byte[] digest = md.digest();
        StringBuilder sb = new StringBuilder();
        for (byte aDigest : digest) {
            if ((0xff & aDigest) < 0x10) {
                sb.append('0');
            }
            sb.append(Integer.toHexString(0xff & aDigest));
        }
        this.md5 = sb.toString();

        // insert the chunks
        final GridFSFile gridFSFile = this;
        chunkCollection.insertMany(documents, new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    // ask the db for the md5
                    queryMD5(new SingleResultCallback<String>() {
                        @Override
                        public void onResult(final String result, final Throwable t) {
                            if (t != null) {
                                // failed to get md5
                                callback.onResult(null, t);
                            } else {
                                // compare MD5
                                if (!md5.equals(result)) {
                                    // TODO log a warning? raise an exception? probably should be configurable...
                                }
                                // insert the file document
                                fileCollection.insertOne(gridFSFile, callback);
                            }
                        }
                    });
                }
            }
        });
    }

    /**
     * Query the db for the md5 of previously inserted chunks.
     *
     * @param callback the callback that is completed once the md5 is retrieved.
     */
    private void queryMD5(final SingleResultCallback<String> callback) {
        Document cmd = new Document("filemd5", id);
        cmd.put("root", gridFS.getBucketName());
        gridFS.getDatabase().executeCommand(cmd, new SingleResultCallback<Document>() {

            @Override
            public void onResult(final Document result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    callback.onResult(result.getString("md5"), null);
                }
            }
        });
    }

    /**
     * Set the chunkSize.
     *
     * @param chunkSize as int
     */
    public void setChunkSize(final int chunkSize) {
        if (length > 0) {
            throw new RuntimeException("chunkSize may not bo modified after data has been written");
        }
        this.chunkSize = chunkSize;
    }

    /**
     * Get all bytes for this GridFSFile from the db.
     *
     * @param dataCallback callback that is completed whenever new data is available
     * @param endCallback callback that is completed when all data has been retrieved
     */
    public void getBytes(final SingleResultCallback<byte[]> dataCallback,
                         final SingleResultCallback<Void> endCallback) {

        GridFSResultCallback callback = new GridFSResultCallback(0, 0, length,
            dataCallback, endCallback);
        fetchChunk(callback);
    }

    /**
     * Get a range of bytes for this GridFSFile from the db.
     *
     * @param start the first byte to retrieve
     * @param end the last byte to retrieve
     * @param dataCallback callback that is completed whenever new data is available
     * @param endCallback callback that is completed when all data has been retrieved
     */
    public void getBytes(final int start, final long end, final SingleResultCallback<byte[]> dataCallback,
                         final SingleResultCallback<Void> endCallback) {
        Integer startChunkIndex = start / chunkSize;
        Integer bytesOffset = start % chunkSize;
        Long bytesToRead = end - start;

        GridFSResultCallback callback = new GridFSResultCallback(startChunkIndex, bytesOffset, bytesToRead,
            dataCallback, endCallback);

        fetchChunk(callback);
    }

    /**
     * fetch a chunk from db.
     *
     * @param callback callback that is completed when the data is available. This callback also contains the
     *                 information about the current chunk to fetch
     */
    private void fetchChunk(final GridFSResultCallback callback) {

        Document query = new Document();
        query.put("files_id", id);
        query.put("n", callback.getChunkIndex());

        gridFS.getChunkCollection()
                .find(query)
                .first(new SingleResultCallback<Document>() {

                    @Override
                    public void onResult(final Document result, final Throwable t) {

                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            Binary binary = (Binary) result.get("data");
                            byte[] bytes = binary.getData();
                            callback.onResult(bytes, null);
                        }
                    }
                });
    }

    private class GridFSResultCallback implements SingleResultCallback<byte[]> {

        private int bytesOffset;
        private int chunkIndex;
        private long bytesToRead;
        private SingleResultCallback<Void> endCallback;
        private SingleResultCallback<byte[]> dataCallback;

        public GridFSResultCallback(final int chunkIndex, final int bytesOffset, final long bytesToRead,
                                    final SingleResultCallback<byte[]> dataCallback,
                                    final SingleResultCallback<Void> endCallback) {

            this.chunkIndex = chunkIndex;
            this.bytesOffset = bytesOffset;
            this.bytesToRead = bytesToRead;
            this.dataCallback = dataCallback;
            this.endCallback = endCallback;
        }

        @Override
        public void onResult(final byte[] result, final Throwable t) {

            if (t != null) {
                // call the endCallback and forward the exception
                endCallback.onResult(null, t);
            } else {
                int bytesInChunk = result.length - bytesOffset;
                int bytesRead = (int) Math.min(bytesInChunk, bytesToRead);

                this.bytesToRead -= bytesRead;
                dataCallback.onResult(Arrays.copyOfRange(result, bytesOffset, bytesRead), null);

                if (this.bytesToRead > 0) {
                    // fetch another chunk
                    fetchChunk(new GridFSResultCallback(chunkIndex + 1, 0, bytesToRead, dataCallback, endCallback));
                } else {
                    // just call the callback to tell the client we are done
                    endCallback.onResult(null, null);
                }
            }
        }


        /**
         * Get the chunkIndex.
         *
         * @return index as int
         */
        int getChunkIndex() {
            return chunkIndex;
        }
    }
}
