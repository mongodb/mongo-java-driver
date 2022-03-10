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

import com.mongodb.benchmark.framework.Benchmark;
import com.mongodb.internal.connection.PowerOfTwoBufferPool;
import org.bson.BsonBinaryWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.json.JsonReader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public abstract class AbstractBsonDocumentBenchmark<T> extends Benchmark {

    protected final PowerOfTwoBufferPool bufferPool = PowerOfTwoBufferPool.DEFAULT;
    protected final Codec<T> codec;

    private final String name;
    private final String resourcePath;

    protected T document;
    protected byte[] documentBytes;
    private int fileLength;

    public AbstractBsonDocumentBenchmark(final String name, final String resourcePath, final Codec<T> codec) {
        this.name = name;
        this.resourcePath = resourcePath;
        this.codec = codec;
    }

    public void setUp() throws IOException {
        byte[] bytes = readAllBytesFromRelativePath(resourcePath);

        fileLength = bytes.length;

        document = codec.decode(new JsonReader(new String(bytes, StandardCharsets.UTF_8)),
                DecoderContext.builder().build());
        documentBytes = getDocumentAsBuffer(document);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getBytesPerRun() {
        return fileLength * NUM_INTERNAL_ITERATIONS;
    }

    private byte[] getDocumentAsBuffer(final T document) throws IOException {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        codec.encode(new BsonBinaryWriter(buffer), document, EncoderContext.builder().build());

        ByteArrayOutputStream baos = new ByteArrayOutputStream(buffer.getSize());
        buffer.pipe(baos);
        return baos.toByteArray();
    }
}
