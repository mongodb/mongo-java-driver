/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.gridfs.model;

import com.mongodb.MongoGridFSException;
import com.mongodb.lang.Nullable;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.Objects;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * The GridFSFile
 *
 * @since 3.1
 */
public final class GridFSFile {
    private final BsonValue id;
    private final String filename;
    private final long length;
    private final int chunkSize;
    private final Date uploadDate;

    // Optional values
    private final Document metadata;

    /**
     * Creates a new GridFSFile
     *
     * @param id the id of the file
     * @param filename the filename
     * @param length the length, in bytes of the file
     * @param chunkSize the chunkSize, in bytes of the file
     * @param uploadDate the upload date of the file
     * @param metadata the optional metadata for the file
     */
    public GridFSFile(final BsonValue id, final String filename, final long length, final int chunkSize, final Date uploadDate,
                      @Nullable final Document metadata) {
        this.id = notNull("id", id);
        this.filename = notNull("filename", filename);
        this.length = notNull("length", length);
        this.chunkSize = notNull("chunkSize", chunkSize);
        this.uploadDate = notNull("uploadDate", uploadDate);
        this.metadata = metadata != null && metadata.isEmpty() ? null : metadata;
    }

    /**
     * The {@link ObjectId} for this file.
     * <p>
     * Throws a MongoGridFSException if the file id is not an ObjectId.
     *
     * @return the id for this file.
     */
    public ObjectId getObjectId() {
        if (!id.isObjectId()) {
            throw new MongoGridFSException("Custom id type used for this GridFS file");
        }
        return id.asObjectId().getValue();
    }

    /**
     * The {@link BsonValue} id for this file.
     *
     * @return the id for this file
     */
    public BsonValue getId() {
        return id;
    }

    /**
     * The filename
     *
     * @return the filename
     */
    public String getFilename() {
        return filename;
    }

    /**
     * The length, in bytes of this file
     *
     * @return the length, in bytes of this file
     */
    public long getLength() {
        return length;
    }

    /**
     * The size, in bytes, of each data chunk of this file
     *
     * @return the size, in bytes, of each data chunk of this file
     */
    public int getChunkSize() {
        return chunkSize;
    }

    /**
     * The date and time this file was added to GridFS
     *
     * @return the date and time this file was added to GridFS
     */
    public Date getUploadDate() {
        return uploadDate;
    }

    /**
     * Any additional metadata stored along with the file
     *
     * @return the metadata document or null
     */
    @Nullable
    public Document getMetadata() {
        return metadata;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)  {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GridFSFile that = (GridFSFile) o;

        if (!Objects.equals(id, that.id)) {
            return false;
        }
        if (!filename.equals(that.filename)) {
            return false;
        }
        if (length != that.length) {
            return false;
        }
        if (chunkSize != that.chunkSize) {
            return false;
        }
        if (!uploadDate.equals(that.uploadDate)) {
            return false;
        }
        if (!Objects.equals(metadata, that.metadata)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + filename.hashCode();
        result = 31 * result + (int) (length ^ (length >>> 32));
        result = 31 * result + chunkSize;
        result = 31 * result + uploadDate.hashCode();
        result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "GridFSFile{"
                + "id=" + id
                + ", filename='" + filename + '\''
                + ", length=" + length
                + ", chunkSize=" + chunkSize
                + ", uploadDate=" + uploadDate
                + ", metadata=" + metadata
                + '}';
    }
}
