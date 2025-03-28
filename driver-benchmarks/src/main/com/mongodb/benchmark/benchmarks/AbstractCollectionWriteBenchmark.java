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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public abstract class AbstractCollectionWriteBenchmark<T> extends AbstractWriteBenchmark<T> {

    protected MongoCollection<T> collection;
    protected MongoDatabase database;

    private final String name;
    private final Class<T> clazz;

    protected AbstractCollectionWriteBenchmark(final String name,
                                               final String resourcePath,
                                               int numIterations,
                                               int numDocuments,
                                               final Class<T> clazz) {
        super(name, resourcePath, numIterations, numDocuments, clazz);
        this.name = name;
        this.clazz = clazz;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        database = client.getDatabase(DATABASE_NAME);
        collection = database.getCollection(COLLECTION_NAME, clazz);
        database.drop();
    }

    @Override
    public void before() throws Exception {
        super.before();
        collection.drop();
    }

    @Override
    public String getName() {
        return name;
    }
}
