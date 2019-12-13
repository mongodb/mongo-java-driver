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

import com.mongodb.client.gridfs.model.GridFSFile;
import org.reactivestreams.Publisher;

import java.nio.ByteBuffer;

/**
 * A GridFS Publisher for downloading data from GridFS
 *
 * <p>Provides the {@code GridFSFile} for the file to being downloaded as well as a way to control the batchsize.</p>
 *
 * @since 1.13
 */
public interface GridFSDownloadPublisher extends Publisher<ByteBuffer> {

    /**
     * Gets the corresponding {@link GridFSFile} for the file being downloaded
     *
     * @return a Publisher with a single element, the corresponding GridFSFile for the file being downloaded
     */
    Publisher<GridFSFile> getGridFSFile();

    /**
     * The preferred number of bytes per {@code ByteBuffer} returned by the {@code Publisher}.
     *
     * <p>Allows for larger than chunk size ByteBuffers. The actual chunk size of the data stored in MongoDB is the smallest allowable
     * {@code ByteBuffer} size.</p>
     *
     * <p>Can be used to control the memory consumption of this {@code Publisher}. The smaller the bufferSizeBytes the lower the memory
     * consumption and higher latency.</p>
     *
     * <p>Note: Must be set before the Publisher is subscribed to.</p>
     *
     * @param bufferSizeBytes the preferred buffer size in bytes to use per {@code ByteBuffer} in the {@code Publisher}, defaults to chunk
     *                        size.
     * @return this
     */
    GridFSDownloadPublisher bufferSizeBytes(int bufferSizeBytes);
}
