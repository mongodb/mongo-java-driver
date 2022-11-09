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

package com.mongodb.client.gridfs;

import com.mongodb.annotations.NotThreadSafe;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

import java.io.OutputStream;

/**
 * A GridFS OutputStream for uploading data into GridFS
 *
 * <p>Provides the {@code id} for the file to be uploaded as well as the {@code write} methods of a {@link OutputStream}</p>
 *
 * <p>This implementation of a {@code OutputStream} will not throw {@link java.io.IOException}s. However, it  will throw a
 * {@link com.mongodb.MongoException} if there is an error writing to MongoDB.</p>
 *
 * @since 3.1
 */
@NotThreadSafe
public abstract class GridFSUploadStream extends OutputStream {

    /**
     * Gets the {@link ObjectId} for the file to be uploaded or throws an error if an alternative BsonType has been used for the id.
     * <p>
     * Throws a MongoGridFSException if the file id is not an ObjectId.
     * @return the ObjectId for the file to be uploaded
     */
    public abstract ObjectId getObjectId();

    /**
     * Gets the {@link BsonValue} for the file to be uploaded
     *
     * @return the BsonValue for the file to be uploaded
     */
    public abstract BsonValue getId();

    /**
     * Aborts the upload and deletes any data.
     */
    public abstract void abort();

    @Override
    public abstract void write(int b);

    @Override
    public abstract void write(byte[] b);

    @Override
    public abstract void write(byte[] b, int off, int len);

    @Override
    public void flush() {}

    @Override
    public abstract void close();
}
