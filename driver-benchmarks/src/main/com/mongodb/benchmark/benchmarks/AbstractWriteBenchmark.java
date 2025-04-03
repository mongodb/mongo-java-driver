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

import com.mongodb.client.model.Filters;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.conversions.Bson;
import org.bson.json.JsonReader;

import java.nio.charset.StandardCharsets;

public abstract class AbstractWriteBenchmark<T> extends AbstractMongoBenchmark {
    protected static final Bson EMPTY_FILTER = Filters.empty();
    private final String resourcePath;
    private final Class<T> clazz;
    private byte[] bytes;
    protected int fileLength;
    protected T document;
    protected int numInternalIterations;
    protected int numDocuments;

    protected AbstractWriteBenchmark(final String name,
                                     final String resourcePath,
                                     int numInternalIterations,
                                     int numDocuments,
                                     final Class<T> clazz) {
        super(name);
        this.resourcePath = resourcePath;
        this.clazz = clazz;
        this.numInternalIterations = numInternalIterations;
        this.numDocuments = numDocuments;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        bytes = readAllBytesFromRelativePath(resourcePath);
        fileLength = bytes.length;
        Codec<T> codec = client.getCodecRegistry().get(clazz);
        document = codec.decode(new JsonReader(new String(bytes, StandardCharsets.UTF_8)), DecoderContext.builder().build());
    }

    protected T createDocument() {
        Codec<T> codec = client.getCodecRegistry().get(clazz);
        return codec.decode(new JsonReader(new String(bytes, StandardCharsets.UTF_8)), DecoderContext.builder().build());
    }

    @Override
    public int getBytesPerRun() {
        return fileLength * numInternalIterations * numDocuments;
    }
}
