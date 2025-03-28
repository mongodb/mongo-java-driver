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
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;

import java.util.ArrayList;
import java.util.List;

public class MixedCollectionBulkWriteBenchmark<T> extends AbstractCollectionWriteBenchmark<T> {
    private final List<WriteModel<T>> modelList;

    public MixedCollectionBulkWriteBenchmark(final String resourcePath, final int numDocuments, final Class<T> clazz) {
        // numDocuments * 2 aligns with bytes transferred (insertOne + replaceOne documents)
        super("Small doc Collection BulkWrite Mixed Operations", resourcePath, 1, numDocuments * 2, clazz);
        this.modelList = new ArrayList<>(numDocuments * 3);
    }


    @Override
    public void before() throws Exception {
        super.before();
        database.createCollection(COLLECTION_NAME);

        modelList.clear();
        for (int i = 0; i < numDocuments / 2; i++) {
            modelList.add(new InsertOneModel<>((createDocument())));
            modelList.add(new ReplaceOneModel<>(EMPTY_FILTER, createDocument()));
            modelList.add(new DeleteOneModel<>(EMPTY_FILTER));
        }
    }

    @Override
    public void run() {
        collection.bulkWrite(modelList);
    }
}
