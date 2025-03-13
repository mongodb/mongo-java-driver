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

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.List;

public class GridFSUploadBenchmark extends AbstractGridFSBenchmark {
    private static final String TEST_NAME = "GridFS upload";

    public GridFSUploadBenchmark(final List<String> tags, final String resourcePath) {
        super(tags, TEST_NAME, resourcePath);
    }

    public GridFSUploadBenchmark(final String resourcePath) {
        super(Collections.emptyList(), TEST_NAME, resourcePath);
    }

    @Override
    public String getName() {
        return "GridFS upload";
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        bucket.uploadFromStream("gridfstest", new ByteArrayInputStream(fileBytes));
    }

    @Override
    public void before() throws Exception {
        super.before();
        database.drop();
        bucket.uploadFromStream("small", new ByteArrayInputStream(new byte[1]));
    }

    @Override
    public void run() {
        bucket.uploadFromStream("gridfstest", new ByteArrayInputStream(fileBytes));
    }
}
