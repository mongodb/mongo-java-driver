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

import com.mongodb.client.model.UpdateOptions;
import org.bson.BsonDocumentWrapper;
import org.bson.Document;

public class UpsertOneBenchmark<T> extends AbstractInsertBenchmark<T> {
    private final int numIterations;
    private final IdRemover<T> idRemover;

    public UpsertOneBenchmark(final String name, final String resourcePath, final int numIterations, final Class<T> clazz,
                              final IdRemover<T> idRemover) {
        super(name + " doc upsertOne", resourcePath, clazz);
        this.numIterations = numIterations;
        this.idRemover = idRemover;
    }

    @Override
    public void run() {
        for (int i = 0; i < numIterations; i++) {
            Object id = idRemover.removeId(document);
            collection.updateOne(new Document("_id", id),
                                 BsonDocumentWrapper.asBsonDocument(new Document("$set", document), collection.getCodecRegistry()),
                                 new UpdateOptions().upsert(true));
        }
    }

    @Override
    public int getBytesPerRun() {
        return fileLength * numIterations;
    }

}
