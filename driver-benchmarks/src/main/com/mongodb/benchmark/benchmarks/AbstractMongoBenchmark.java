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

import com.mongodb.benchmark.framework.Benchmark;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public abstract class AbstractMongoBenchmark extends Benchmark {

    protected static final int GRIDFS_READING_THREAD_POOL_SIZE = 8;
    protected static final int MONGODB_READING_THREAD_POOL_SIZE = 8;
    protected static final int MONGODB_WRITING_THREAD_POOL_SIZE = 8;
    protected static final int FILE_WRITING_THREAD_POOL_SIZE = 2;
    protected static final int FILE_READING_THREAD_POOL_SIZE = 4;

    protected static final int ONE_MB = 1000000;

    protected static final String DATABASE_NAME = "perftest";
    protected static final String COLLECTION_NAME = "corpus";


    protected MongoClient client;

    public void setUp() throws Exception {
       client = MongoClients.create();
    }

    @Override
    public void tearDown() throws Exception {
        client.close();
    }

}
