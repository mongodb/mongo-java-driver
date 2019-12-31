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

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;

public abstract class AbstractGridFSBenchmark extends AbstractMongoBenchmark {
    private final String resourcePath;
    protected MongoDatabase database;
    protected GridFSBucket bucket;
    protected byte[] fileBytes;

    public AbstractGridFSBenchmark(final String resourcePath) {
        this.resourcePath = resourcePath;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        database = client.getDatabase(DATABASE_NAME);
        bucket = GridFSBuckets.create(database);
        fileBytes = readAllBytesFromRelativePath(resourcePath);
        database.drop();
    }

    @Override
    public int getBytesPerRun() {
        return fileBytes.length;
    }
}
