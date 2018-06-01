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

package com.mongodb.benchmark.framework;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

public abstract class Benchmark {

    protected static final int NUM_INTERNAL_ITERATIONS = 10000;
    static final String TEST_DATA_SYSTEM_PROPERTY_NAME = "org.mongodb.benchmarks.data";

    public void setUp() throws Exception {
    }

    public void tearDown() throws Exception {
    }

    public void before() throws Exception {
    }

    public void after() throws Exception {
    }

    public abstract String getName();

    public abstract void run() throws Exception;

    public abstract int getBytesPerRun();

    protected byte[] readAllBytesFromRelativePath(final String relativePath) throws IOException {
        return Files.readAllBytes(Paths.get(getResourceRoot() + relativePath));
    }

    protected Reader readFromRelativePath(final String relativePath) throws IOException {
        return new FileReader(getResourceRoot() + relativePath);
    }

    protected InputStream streamFromRelativePath(final String relativePath) throws IOException {
        return new FileInputStream(getResourceRoot() + relativePath);
    }

    private String getResourceRoot() {
        return System.getProperty(TEST_DATA_SYSTEM_PROPERTY_NAME);
    }
}
