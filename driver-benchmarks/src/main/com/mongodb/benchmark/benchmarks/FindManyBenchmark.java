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

import java.util.Collections;
import java.util.List;

public class FindManyBenchmark<T> extends AbstractFindBenchmark<T> {

    private static final String TEST_NAME = "Find many and empty the cursor";

    public FindManyBenchmark(final String resourcePath, final Class<T> clazz) {
        this(Collections.emptyList(), resourcePath, clazz);
    }

    public FindManyBenchmark(final List<String> tags, final String resourcePath, final Class<T> clazz) {
        super(tags, TEST_NAME, resourcePath, clazz);
    }

    @Override
    public void run() {
        try (MongoCursor<T> cursor = collection.find().iterator()) {
            while (cursor.hasNext()) {
                cursor.next();
            }
        }
    }
}
