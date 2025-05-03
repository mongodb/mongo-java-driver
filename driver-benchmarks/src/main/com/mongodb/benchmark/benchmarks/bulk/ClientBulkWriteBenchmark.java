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

package com.mongodb.benchmark.benchmarks.bulk;

import com.mongodb.benchmark.benchmarks.AbstractCollectionWriteBenchmark;
import com.mongodb.client.model.bulk.ClientNamespacedInsertOneModel;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;

import java.util.ArrayList;
import java.util.List;

public class ClientBulkWriteBenchmark<T> extends AbstractCollectionWriteBenchmark<T> {
    private final List<ClientNamespacedInsertOneModel> modelList;

    public ClientBulkWriteBenchmark(final String name, final String resourcePath, final int numDocuments, final Class<T> clazz) {
        super(name + " doc Client BulkWrite insert", resourcePath, 1, numDocuments, clazz);
        modelList = new ArrayList<>(numDocuments);
    }

    @Override
    public void before() throws Exception {
        super.before();
        database.createCollection(COLLECTION_NAME);

        modelList.clear();
        for (int i = 0; i < numDocuments; i++) {
            modelList.add(ClientNamespacedWriteModel.insertOne(NAMESPACE, createDocument()));
        }
    }

    @Override
    public void run() {
        client.bulkWrite(modelList);
    }
}
