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

package com.mongodb.gridfs;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBObjectCodecProvider;
import com.mongodb.MongoException;
import org.bson.BSONObject;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;

import java.io.StringWriter;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

/**
 * The abstract class representing a GridFS file.
 *
 * @mongodb.driver.manual core/gridfs/ GridFS
 */
public abstract class GridFSFile implements DBObject {

    private static final CodecRegistry DEFAULT_REGISTRY =
            fromProviders(asList(new ValueCodecProvider(), new BsonValueCodecProvider(), new DBObjectCodecProvider()));

    private static final Set<String> VALID_FIELDS = Collections.unmodifiableSet(new HashSet<>(asList("_id",
            "filename",
            "contentType",
            "length",
            "chunkSize",
            "uploadDate",
            "aliases")));

    final DBObject extra = new BasicDBObject();

    GridFS fs;
    Object id;
    String filename;
    String contentType;
    long length;
    long chunkSize;
    Date uploadDate;

    /**
     * Saves the file entry to the files collection
     *
     * @throws MongoException if there's a failure
     */
    public void save() {
        if (fs == null) {
            throw new MongoException("need fs");
        }
        fs.getFilesCollection().save(this);
    }

    /**
     * Returns the number of chunks that store the file data.
     *
     * @return number of chunks
     */
    public int numChunks() {
        double d = length;
        d = d / chunkSize;
        return (int) Math.ceil(d);
    }

    /**
     * Gets the id.
     *
     * @return the id of the file.
     */
    public Object getId() {
        return id;
    }

    /**
     * Gets the filename.
     *
     * @return the name of the file
     */
    public String getFilename() {
        return filename;
    }

    /**
     * Gets the content type.
     *
     * @return the content type
     */
    public String getContentType() {
        return contentType;
    }

    /**
     * Gets the file's length.
     *
     * @return the length of the file
     */
    public long getLength() {
        return length;
    }

    /**
     * Gets the size of a chunk.
     *
     * @return the chunkSize
     */
    public long getChunkSize() {
        return chunkSize;
    }

    /**
     * Gets the upload date.
     *
     * @return the date
     */
    public Date getUploadDate() {
        return uploadDate;
    }

    /**
     * Gets the aliases from the metadata. note: to set aliases, call {@link #put(String, Object)} with {@code "aliases" , List<String>}.
     *
     * @return list of aliases
     */
    @SuppressWarnings("unchecked")
    public List<String> getAliases() {
        return (List<String>) extra.get("aliases");
    }

    /**
     * Gets the file metadata.
     *
     * @return the metadata
     */
    public DBObject getMetaData() {
        return (DBObject) extra.get("metadata");
    }

    /**
     * Gets the file metadata.
     *
     * @param metadata metadata to be set
     */
    public void setMetaData(final DBObject metadata) {
        extra.put("metadata", metadata);
    }

    @Override
    public Object put(final String key, final Object v) {
        if (key == null) {
            throw new RuntimeException("key should never be null");
        } else if (key.equals("_id")) {
            id = v;
        } else if (key.equals("filename")) {
            filename = v == null ? null : v.toString();
        } else if (key.equals("contentType")) {
            contentType = (String) v;
        } else if (key.equals("length")) {
            length = ((Number) v).longValue();
        } else if (key.equals("chunkSize")) {
            chunkSize = ((Number) v).longValue();
        } else if (key.equals("uploadDate")) {
            uploadDate = (Date) v;
        } else {
            extra.put(key, v);
        }
        return v;
    }

    @Override
    public Object get(final String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key should never be null");
        } else if (key.equals("_id")) {
            return id;
        } else if (key.equals("filename")) {
            return filename;
        } else if (key.equals("contentType")) {
            return contentType;
        } else if (key.equals("length")) {
            return length;
        } else if (key.equals("chunkSize")) {
            return chunkSize;
        } else if (key.equals("uploadDate")) {
            return uploadDate;
        }
        return extra.get(key);
    }

    @Override
    public boolean containsField(final String s) {
        return keySet().contains(s);
    }

    @Override
    public Set<String> keySet() {
        Set<String> keys = new HashSet<>();
        keys.addAll(VALID_FIELDS);
        keys.addAll(extra.keySet());
        return keys;
    }

    @Override
    public boolean isPartialObject() {
        return false;
    }

    @Override
    public void markAsPartialObject() {
        throw new MongoException("Can't load partial GridFSFile file");
    }

    @Override
    public String toString() {
        JsonWriter writer = new JsonWriter(new StringWriter(), JsonWriterSettings.builder().build());
        DEFAULT_REGISTRY.get(GridFSFile.class).encode(writer, this,
                EncoderContext.builder().isEncodingCollectibleDocument(true).build());
        return writer.getWriter().toString();
    }

    /**
     * Sets the GridFS associated with this file.
     *
     * @param fs gridFS instance
     */
    protected void setGridFS(final GridFS fs) {
        this.fs = fs;
    }

    /**
     * Gets the GridFS associated with this file
     *
     * @return gridFS instance
     */
    protected GridFS getGridFS() {
        return this.fs;
    }

    @Override
    public void putAll(final BSONObject o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(@SuppressWarnings("rawtypes") final Map m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<?, ?> toMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object removeField(final String key) {
        throw new UnsupportedOperationException();
    }
}
