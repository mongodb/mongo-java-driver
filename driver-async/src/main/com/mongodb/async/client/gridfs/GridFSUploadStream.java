/*
 * Copyright 2015 MongoDB, Inc.
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

package com.mongodb.async.client.gridfs;

import com.mongodb.async.SingleResultCallback;
import org.bson.types.ObjectId;

/**
 * A GridFS OutputStream for uploading data into GridFS
 *
 * <p>Provides the {@code id} for the file to be uploaded as well as the {@code write} methods of a {@link AsyncOutputStream}</p>
 *
 * @since 3.3
 */
public interface GridFSUploadStream extends AsyncOutputStream {

    /**
     * Gets the {@link ObjectId} for the file to be uploaded
     *
     * @return the ObjectId for the file to be uploaded
     */
    ObjectId getFileId();

    /**
     * Aborts the upload and deletes any data.
     *
     * @param callback the callback that is triggered when the abort and cleanup has finished
     */
    void abort(SingleResultCallback<Void> callback);

}
