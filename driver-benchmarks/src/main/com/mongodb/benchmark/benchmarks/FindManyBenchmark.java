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

import com.mongodb.client.MongoCursor;

public class FindManyBenchmark<T> extends AbstractFindBenchmark<T> {
    public FindManyBenchmark(final String resourcePath, final Class<T> clazz) {
        super("Find many and empty the cursor", resourcePath, clazz);
    }

    @Override
    public void run() {
         MongoCursor<T> cursor = collection.find().iterator();
         try {
             while (cursor.hasNext()) {
                 cursor.next();
             }
         } finally {
             cursor.close();
         }
    }
}
