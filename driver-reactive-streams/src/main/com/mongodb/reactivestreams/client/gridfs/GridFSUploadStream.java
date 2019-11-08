/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.gridfs;

import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;

/**
 * A GridFS OutputStream for uploading data into GridFS
 *
 * <p>Provides the {@code id} for the file to be uploaded as well as the {@code write} methods of a {@link AsyncOutputStream}</p>
 *
 * @since 1.3
 */
public interface GridFSUploadStream extends AsyncOutputStream {

    /**
     * Gets the {@link ObjectId} for the file to be uploaded
     *
     * Throws a {@link com.mongodb.MongoGridFSException} if the file id is not an ObjectId.
     *
     * @return the ObjectId for the file to be uploaded
     */
    ObjectId getObjectId();

    /**
     * The {@link BsonValue} id for this file.
     *
     * @return the id for this file
     */
    BsonValue getId();

    /**
     * Aborts the upload and deletes any data.
     *
     * @return a publisher with a single element, signifying the abort and cleanup has finished
     */
    Publisher<Void> abort();

}
