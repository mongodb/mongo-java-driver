package org.mongodb.file;

import org.mongodb.ReadPreference;
import org.mongodb.WriteConcern;
import org.mongodb.file.util.ChunkSize;

public final class MongoFileStoreConfig {

    public static final ChunkSize DEFAULT_CHUNKSIZE = ChunkSize.medium_256K;

    private String bucket = "fileStore";
    private WriteConcern writeConcern = WriteConcern.JOURNALED;
    private ReadPreference readPreference = ReadPreference.primary();
    private boolean enableCompression = true;
    private ChunkSize chunkSize = DEFAULT_CHUNKSIZE;
    private boolean asyncDeletes = true;

    private MongoFileStoreConfig() {
        // use Builder
    }

    public String getBucket() {

        return bucket;
    }

    private void setBucket(final String bucket) {

        this.bucket = bucket;
    }

    public WriteConcern getWriteConcern() {

        return writeConcern;
    }

    private void setWriteConcern(final WriteConcern writeConcern) {

        this.writeConcern = writeConcern;
    }

    public ReadPreference getReadPreference() {

        return readPreference;
    }

    private void setReadPreference(final ReadPreference readPreference) {

        this.readPreference = readPreference;
    }

    public boolean isEnableCompression() {

        return enableCompression;
    }

    private void setEnableCompression(final boolean enableCompression) {

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
    private void setChunkSize(final ChunkSize chunkSize) {

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
    private void setAsyncDeletes(final boolean asyncDeletes) {

        this.asyncDeletes = asyncDeletes;
    }

    @Override
    public String toString() {

        return String.format(
                "MongoFileStoreConfig [bucket=%s, chunkSize=%s, enableCompression=%s, writeConcern=%s, readPreference=%s]",
                bucket, chunkSize, enableCompression, writeConcern, readPreference);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private MongoFileStoreConfig config = new MongoFileStoreConfig();

        public MongoFileStoreConfig build() {
            return config;
        }

        // setters
        public Builder asyncDeletes(final boolean value) {
            config.setAsyncDeletes(value);
            return this;
        }

        public Builder bucket(final String value) {
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException("bucket name cannot be nul of empty");
            }
            config.setBucket(value);
            return this;
        }

        public Builder chunkSize(final ChunkSize value) {
            config.setChunkSize(value);
            return this;
        }

        public Builder enableCompression(final boolean value) {
            config.setEnableCompression(value);
            return this;
        }

        public Builder readPreference(final ReadPreference value) {
            config.setReadPreference(value);
            return this;
        }

        public Builder writeConcern(final WriteConcern value) {
            config.setWriteConcern(value);
            return this;
        }
    }

}
