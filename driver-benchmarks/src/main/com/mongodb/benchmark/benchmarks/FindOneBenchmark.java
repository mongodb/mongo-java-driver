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

import com.mongodb.benchmark.framework.BenchmarkRunner;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

public class FindOneBenchmark<T> extends AbstractFindBenchmark<T> {

    public FindOneBenchmark(final String resourcePath, Class<T> clazz) {
        super("Find one by ID", resourcePath, clazz);
    }

    @Override
    public void run() {
        for (int i = 0; i < NUM_INTERNAL_ITERATIONS; i++) {
            collection.find(new BsonDocument("_id", new BsonInt32(i))).first();
        }
    }

    public static void main(String[] args) throws Exception {
        new BenchmarkRunner(new FindOneBenchmark<>("/benchmarks/TWEET.json", BsonDocument.class), 0, 1).run();
    }
}
