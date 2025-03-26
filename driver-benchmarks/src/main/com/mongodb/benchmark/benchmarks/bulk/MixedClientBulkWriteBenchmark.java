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

import com.mongodb.MongoNamespace;
import com.mongodb.benchmark.benchmarks.AbstractMongoBenchmark;
import com.mongodb.benchmark.benchmarks.AbstractWriteBenchmark;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.bulk.ClientNamespacedWriteModel.deleteOne;
import static com.mongodb.client.model.bulk.ClientNamespacedWriteModel.insertOne;
import static com.mongodb.client.model.bulk.ClientNamespacedWriteModel.replaceOne;

public class MixedClientBulkWriteBenchmark<T> extends AbstractWriteBenchmark<T> {
    private static final int NAMESPACES_COUNT = 10;
    private MongoDatabase database;
    private final List<ClientNamespacedWriteModel> modelList;
    private List<MongoNamespace> namespaces;

    public MixedClientBulkWriteBenchmark(final String resourcePath, final int numDocuments, final Class<T> clazz) {
        super("Small doc Client BulkWrite Mixed Operations", resourcePath, 1, numDocuments * 3, clazz);
        modelList = new ArrayList<>(super.numDocuments);
        namespaces = new ArrayList<>(NAMESPACES_COUNT);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        database = client.getDatabase(DATABASE_NAME);
        database.drop();

        namespaces = new ArrayList<>();
        for (int i = 1; i <= NAMESPACES_COUNT; i++) {
            namespaces.add(new MongoNamespace(AbstractMongoBenchmark.DATABASE_NAME, AbstractMongoBenchmark.COLLECTION_NAME + "_" + i));
        }
    }

    @Override
    public void before() throws Exception {
        super.before();
        database.drop();
        database = client.getDatabase(DATABASE_NAME);

        for (MongoNamespace namespace : namespaces) {
            database.createCollection(namespace.getCollectionName());
        }

        modelList.clear();
        for (int i = 0; i < numDocuments; i++) {
            MongoNamespace namespace = namespaces.get(i % NAMESPACES_COUNT);
            modelList.add(insertOne(
                    namespace,
                    createDocument()));
            modelList.add(replaceOne(
                    namespace, EMPTY_FILTER,
                    createDocument()));
            modelList.add(deleteOne(
                    namespace, EMPTY_FILTER));
        }
    }

    @Override
    public void run() {
        client.bulkWrite(modelList);
    }
}
