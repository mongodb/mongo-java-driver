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

package com.mongodb.internal.async.client.gridfs;

import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.internal.async.SingleResultCallback;

/**
 * A GridFS InputStream for downloading data from GridFS
 *
 * <p>Provides the {@code GridFSFile} for the file to being downloaded as well as the {@code read} methods of a {@link AsyncInputStream}</p>
 */
public interface AsyncGridFSDownloadStream extends AsyncInputStream {

    /**
     * Gets the corresponding {@link GridFSFile} for the file being downloaded
     *
     * @param callback that returns the corresponding GridFSFile for the file being downloaded
     */
    void getGridFSFile(SingleResultCallback<GridFSFile> callback);

    /**
     * Sets the number of chunks to return per batch.
     *
     * <p>Can be used to control the memory consumption of this InputStream. The smaller the batchSize the lower the memory consumption
     * and higher latency.</p>
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    AsyncGridFSDownloadStream batchSize(int batchSize);
}
