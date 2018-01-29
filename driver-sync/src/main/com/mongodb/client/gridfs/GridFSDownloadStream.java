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
import com.mongodb.client.gridfs.model.GridFSFile;

import java.io.InputStream;

/**
 * A GridFS InputStream for downloading data from GridFS
 *
 * <p>Provides the {@code GridFSFile} for the file to being downloaded as well as the {@code read} methods of a {@link InputStream}</p>
 *
 * <p>This implementation of a {@code InputStream} will not throw {@link java.io.IOException}s. However, it  will throw a
 * {@link com.mongodb.MongoException} if there is an error reading from MongoDB.</p>
 *
 * @since 3.1
 */
@NotThreadSafe
public abstract class GridFSDownloadStream extends InputStream {

    /**
     * Gets the corresponding {@link GridFSFile} for the file being downloaded
     *
     * @return the corresponding GridFSFile for the file being downloaded
     */
    public abstract GridFSFile getGridFSFile();

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
    public abstract GridFSDownloadStream batchSize(int batchSize);

    @Override
    public abstract int read();

    @Override
    public abstract int read(byte[] b);

    @Override
    public abstract int read(byte[] b, int off, int len);

    @Override
    public abstract long skip(long n);

    @Override
    public abstract int available();

    /**
     * Marks the current position in this input stream.
     *
     * <p>A subsequent call to the {@code reset} method repositions this stream at the last marked position so that subsequent reads
     * re-read the same bytes.</p>
     */
    public abstract void mark();

    @Override
    public abstract void reset();

    @Override
    public abstract void close();
}
