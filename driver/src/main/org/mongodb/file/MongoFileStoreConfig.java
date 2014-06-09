package org.mongodb.file;

import org.mongodb.ReadPreference;
import org.mongodb.WriteConcern;
import org.mongodb.file.util.ChunkSize;

public class MongoFileStoreConfig {

    public static final ChunkSize DEFAULT_CHUNKSIZE = ChunkSize.medium_256K;

    private String bucket = "mongofs";
    private WriteConcern writeConcern = WriteConcern.JOURNALED;
    private ReadPreference readPreference = ReadPreference.primary();
    private boolean enableCompression = true;
    private ChunkSize chunkSize = DEFAULT_CHUNKSIZE;
    private boolean asyncDeletes = true;

    public MongoFileStoreConfig(final String bucket) {

        this.bucket = bucket;
    }

    public String getBucket() {

        return bucket;
    }

    public void setBucket(final String bucket) {

        this.bucket = bucket;
    }

    public WriteConcern getWriteConcern() {

        return writeConcern;
    }

    public void setWriteConcern(final WriteConcern writeConcern) {

        this.writeConcern = writeConcern;
    }

    public ReadPreference getReadPreference() {

        return readPreference;
    }

    public void setReadPreference(final ReadPreference readPreference) {

        this.readPreference = readPreference;
    }

    public boolean isEnableCompression() {

        return enableCompression;
    }

    public void setEnableCompression(final boolean enableCompression) {

        this.enableCompression = enableCompression;
    }

    public int getChunkSize() {

        return chunkSize.getChunkSize();
    }

    /**
     * Specifies the chunk size to use for data chunks
     * 
     * @param chunkSize
     */
    public void setChunkSize(final ChunkSize chunkSize) {

        this.chunkSize = chunkSize;
    }

    /**
     * Are async deletes allowed
     * 
     * @return true if allowed
     */
    public boolean isAsyncDeletes() {

        return asyncDeletes;
    }

    /**
     * Should async deletes be allowed
     * 
     * @param asyncDeletes
     *            true is the default
     */
    public void setAsyncDeletes(final boolean asyncDeletes) {

        this.asyncDeletes = asyncDeletes;
    }

    @Override
    public String toString() {

        return String.format(
                "MongoFileStoreConfig [bucket=%s, chunkSize=%s, enableCompression=%s, writeConcern=%s, readPreference=%s]",
                bucket, chunkSize, enableCompression, writeConcern, readPreference);
    }

}
