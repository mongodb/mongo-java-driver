/*
 * Copyright 2016-present MongoDB, Inc.
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
 *
 */

package com.mongodb.benchmark.benchmarks;

import com.mongodb.client.gridfs.GridFSDownloadStream;
import org.bson.types.ObjectId;

import java.io.ByteArrayInputStream;

public class GridFSDownloadBenchmark extends AbstractGridFSBenchmark {

    private ObjectId fileId;

    public GridFSDownloadBenchmark(final String resourcePath) {
        super(resourcePath);
    }

    @Override
    public String getName() {
        return "GridFS download";
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        fileId = bucket.uploadFromStream("gridfstest", new ByteArrayInputStream(fileBytes));
    }

    @Override
    public void run() {
        GridFSDownloadStream stream = bucket.openDownloadStream(fileId);
        byte[] chunk = new byte[bucket.getChunkSizeBytes()];
        while (stream.read(chunk) != -1) {
            // discard result
        }
    }
}
