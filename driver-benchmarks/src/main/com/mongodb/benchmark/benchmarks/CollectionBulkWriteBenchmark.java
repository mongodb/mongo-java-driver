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

import com.mongodb.client.model.InsertOneModel;

import java.util.ArrayList;
import java.util.List;

public class CollectionBulkWriteBenchmark<T> extends InsertManyBenchmark<T> {
    private final List<InsertOneModel<T>> documentList;

    public CollectionBulkWriteBenchmark(final String name, final String resourcePath, final int numDocuments, final Class<T> clazz) {
        super(name + " doc Collection BulkWrite insert", resourcePath, numDocuments, clazz);
        documentList = new ArrayList<>(numDocuments);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void before() throws Exception {
        super.before();

        documentList.clear();
        for (int i = 0; i < numDocuments; i++) {
            documentList.add(new InsertOneModel<>((createDocument())));
        }
    }

    @Override
    public void run() {
        collection.bulkWrite(documentList);
    }
}
